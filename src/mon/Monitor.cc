// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "Monitor.h"

#include "osd/OSDMap.h"

#include "MonitorDBStore.h"

#include "msg/Messenger.h"

#include "messages/PaxosServiceMessage.h"
#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonSync.h"
#include "messages/MMonProbe.h"
#include "messages/MMonJoin.h"
#include "messages/MMonPaxos.h"
#include "messages/MRoute.h"
#include "messages/MForward.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"

#include "messages/MAuthReply.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/Timer.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"

#include "include/color.h"
#include "include/ceph_fs.h"
#include "include/str_list.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "MonmapMonitor.h"
#include "PGMonitor.h"
#include "LogMonitor.h"
#include "AuthMonitor.h"

#include "osd/OSDMap.h"

#include "auth/AuthSupported.h"
#include "auth/KeyRing.h"

#include "common/config.h"

#include <sstream>
#include <stdlib.h>
#include <signal.h>

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, const Monitor *mon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name() << ") e" << mon->monmap->get_epoch() << " ";
}

const string Monitor::MONITOR_NAME = "monitor";

CompatSet get_ceph_mon_feature_compat_set()
{
  CompatSet::FeatureSet ceph_mon_feature_compat;
  CompatSet::FeatureSet ceph_mon_feature_ro_compat;
  CompatSet::FeatureSet ceph_mon_feature_incompat;
  ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_BASE);
  return CompatSet(ceph_mon_feature_compat, ceph_mon_feature_ro_compat,
		   ceph_mon_feature_incompat);
}

Monitor::Monitor(CephContext* cct_, string nm, MonitorDBStore *s,
		 Messenger *m, MonMap *map) :
  Dispatcher(cct_),
  name(nm),
  rank(-1), 
  messenger(m),
  lock("Monitor::lock"),
  timer(cct_, lock),
  has_ever_joined(false),
  logger(NULL), cluster_logger(NULL), cluster_logger_registered(false),
  monmap(map),
  clog(cct_, messenger, monmap, NULL, LogClient::FLAG_MON),
  key_server(cct),
  auth_supported(cct),
  store(s),
  
  state(STATE_PROBING),
  
  elector(this),
  leader(0),

  // trim & store sync
  sync_role(SYNC_ROLE_NONE),
  trim_lock("Monitor::trim_lock"),
  trim_enable_timer(NULL),
  sync_state(SYNC_STATE_NONE),
  sync_leader(),
  sync_provider(),

  probe_timeout_event(NULL),

  paxos_service(PAXOS_NUM),
  admin_hook(NULL),
  routed_request_tid(0)
{
  rank = -1;

  paxos = new Paxos(this, "paxos"); 

  paxos_service[PAXOS_MDSMAP] = new MDSMonitor(this, paxos, "mdsmap");
  paxos_service[PAXOS_MONMAP] = new MonmapMonitor(this, paxos, "monmap");
  paxos_service[PAXOS_OSDMAP] = new OSDMonitor(this, paxos, "osdmap");
  paxos_service[PAXOS_PGMAP] = new PGMonitor(this, paxos, "pgmap");
  paxos_service[PAXOS_LOG] = new LogMonitor(this, paxos, "log");
  paxos_service[PAXOS_AUTH] = new AuthMonitor(this, paxos, "auth");

  mon_caps = new MonCaps();
  mon_caps->set_allow_all(true);
  mon_caps->text = "allow *";

  exited_quorum = ceph_clock_now(g_ceph_context);
}

PaxosService *Monitor::get_paxos_service_by_name(const string& name)
{
  if (name == "mdsmap")
    return paxos_service[PAXOS_MDSMAP];
  if (name == "monmap")
    return paxos_service[PAXOS_MONMAP];
  if (name == "osdmap")
    return paxos_service[PAXOS_OSDMAP];
  if (name == "pgmap")
    return paxos_service[PAXOS_PGMAP];
  if (name == "logm")
    return paxos_service[PAXOS_LOG];
  if (name == "auth")
    return paxos_service[PAXOS_AUTH];

  assert(0 == "given name does not match known paxos service");
  return NULL;
}

Monitor::~Monitor()
{
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    delete *p;
  
  delete paxos;

  //clean out MonSessionMap's subscriptions
  for (map<string, xlist<Subscription*>* >::iterator i
	 = session_map.subs.begin();
       i != session_map.subs.end();
       ++i) {
    while (!i->second->empty())
      session_map.remove_sub(i->second->front());
    delete i->second;
  }
  //clean out MonSessionMap's sessions
  while (!session_map.sessions.empty()) {
    session_map.remove_session(session_map.sessions.front());
  }
  delete mon_caps;
}

enum {
  l_mon_first = 456000,
  l_mon_last,
};


class AdminHook : public AdminSocketHook {
  Monitor *mon;
public:
  AdminHook(Monitor *m) : mon(m) {}
  bool call(std::string command, bufferlist& out) {
    stringstream ss;
    mon->do_admin_command(command, ss);
    out.append(ss);
    return true;
  }
};

void Monitor::do_admin_command(string command, ostream& ss)
{
  Mutex::Locker l(lock);
  if (command == "mon_status")
    _mon_status(ss);
  else if (command == "quorum_status")
    _quorum_status(ss);
  else if (command == "sync_status")
    _sync_status(ss);
  else if (command == "sync_force")
    _sync_force(ss);
  else if (command.find("add_bootstrap_peer_hint") == 0)
    _add_bootstrap_peer_hint(command, ss);
  else
    assert(0 == "bad AdminSocket command binding");
}

void Monitor::handle_signal(int signum)
{
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got Signal " << sys_siglist[signum] << " ***" << dendl;
  shutdown();
}

int Monitor::init()
{
  lock.Lock();

  dout(1) << "init fsid " << monmap->fsid << dendl;
  
  assert(!logger);
  {
    PerfCountersBuilder pcb(g_ceph_context, "mon", l_mon_first, l_mon_last);
    // ...
    logger = pcb.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
  }

  assert(!cluster_logger);
  {
    PerfCountersBuilder pcb(g_ceph_context, "cluster", l_cluster_first, l_cluster_last);
    pcb.add_u64(l_cluster_num_mon, "num_mon");
    pcb.add_u64(l_cluster_num_mon_quorum, "num_mon_quorum");
    pcb.add_u64(l_cluster_num_osd, "num_osd");
    pcb.add_u64(l_cluster_num_osd_up, "num_osd_up");
    pcb.add_u64(l_cluster_num_osd_in, "num_osd_in");
    pcb.add_u64(l_cluster_osd_epoch, "osd_epoch");
    pcb.add_u64(l_cluster_osd_kb, "osd_kb");
    pcb.add_u64(l_cluster_osd_kb_used, "osd_kb_used");
    pcb.add_u64(l_cluster_osd_kb_avail, "osd_kb_avail");
    pcb.add_u64(l_cluster_num_pool, "num_pool");
    pcb.add_u64(l_cluster_num_pg, "num_pg");
    pcb.add_u64(l_cluster_num_pg_active_clean, "num_pg_active_clean");
    pcb.add_u64(l_cluster_num_pg_active, "num_pg_active");
    pcb.add_u64(l_cluster_num_pg_peering, "num_pg_peering");
    pcb.add_u64(l_cluster_num_object, "num_object");
    pcb.add_u64(l_cluster_num_object_degraded, "num_object_degraded");
    pcb.add_u64(l_cluster_num_object_unfound, "num_object_unfound");
    pcb.add_u64(l_cluster_num_bytes, "num_bytes");
    pcb.add_u64(l_cluster_num_mds_up, "num_mds_up");
    pcb.add_u64(l_cluster_num_mds_in, "num_mds_in");
    pcb.add_u64(l_cluster_num_mds_failed, "num_mds_failed");
    pcb.add_u64(l_cluster_mds_epoch, "mds_epoch");
    cluster_logger = pcb.create_perf_counters();
  }

  // open compatset
  {
    bufferlist bl;
    store->get(MONITOR_NAME, COMPAT_SET_LOC, bl);
    if (bl.length()) {
      bufferlist::iterator p = bl.begin();
      ::decode(features, p);
    } else {
      features = get_ceph_mon_feature_compat_set();
    }
    dout(10) << "features " << features << dendl;
  }

  // have we ever joined a quorum?
  has_ever_joined = (store->get(MONITOR_NAME, "joined") != 0);
  dout(10) << "has_ever_joined = " << (int)has_ever_joined << dendl;

  if (!has_ever_joined) {
    // impose initial quorum restrictions?
    list<string> initial_members;
    get_str_list(g_conf->mon_initial_members, initial_members);

    if (initial_members.size()) {
      dout(1) << " initial_members " << initial_members << ", filtering seed monmap" << dendl;

      monmap->set_initial_members(g_ceph_context, initial_members, name, messenger->get_myaddr(),
				  &extra_probe_peers);

      dout(10) << " monmap is " << *monmap << dendl;
    }
  }

  {
    // We have a potentially inconsistent store state in hands. Get rid of it
    // and start fresh.
    bool clear_store = false;
    if (store->get("mon_sync", "in_sync") > 0) {
      dout(1) << __func__ << " clean up potentially inconsistent store state"
	      << dendl;
      clear_store = true;
    }

    if (store->get("mon_sync", "force_sync") > 0) {
      dout(1) << __func__ << " force sync by clearing store state" << dendl;
      clear_store = true;
    }

    if (clear_store) {
      set<string> sync_prefixes = get_sync_targets_names();
      sync_prefixes.insert("mon_sync");
      store->clear(sync_prefixes);
    }
  }

  init_paxos();

  // we need to bootstrap authentication keys so we can form an
  // initial quorum.
  if (authmon()->get_version() == 0) {
    dout(10) << "loading initial keyring to bootstrap authentication for mkfs" << dendl;
    bufferlist bl;
    store->get("mkfs", "keyring", bl);
    KeyRing keyring;
    bufferlist::iterator p = bl.begin();
    ::decode(keyring, p);
    extract_save_mon_key(keyring);
  }

  string keyring_loc;

  if (g_conf->keyring != "keyring")
    keyring_loc = g_conf->keyring;
  else {
    ostringstream os;
    os << g_conf->mon_data << "/keyring";
    keyring_loc = os.str();
  }

  int r = keyring.load(cct, keyring_loc);
  if (r < 0) {
    EntityName mon_name;
    mon_name.set_type(CEPH_ENTITY_TYPE_MON);
    EntityAuth mon_key;
    if (key_server.get_auth(mon_name, mon_key)) {
      dout(1) << "copying mon. key from old db to external keyring" << dendl;
      keyring.add(mon_name, mon_key);
      bufferlist bl;
      keyring.encode_plaintext(bl);
      write_default_keyring(bl);
    } else {
      derr << "unable to load initial keyring " << g_conf->keyring << dendl;
      return r;
    }
  }

  admin_hook = new AdminHook(this);
  AdminSocket* admin_socket = cct->get_admin_socket();
  r = admin_socket->register_command("mon_status", admin_hook,
				     "show current monitor status");
  assert(r == 0);
  r = admin_socket->register_command("quorum_status", admin_hook,
					 "show current quorum status");
  assert(r == 0);
  r = admin_socket->register_command("sync_status", admin_hook,
				     "show current synchronization status");
  assert(r == 0);
  r = admin_socket->register_command("add_bootstrap_peer_hint", admin_hook,
				     "add peer address as potential bootstrap peer for cluster bringup");
  assert(r == 0);

  // i'm ready!
  messenger->add_dispatcher_tail(this);
  messenger->add_dispatcher_head(&clog);
  
  // start ticker
  timer.init();
  new_tick();

  bootstrap();
  
  lock.Unlock();
  return 0;
}

void Monitor::init_paxos()
{
  dout(10) << __func__ << dendl;
  paxos->init();
  // init paxos
  for (int i = 0; i < PAXOS_NUM; ++i) {
    if (paxos->is_consistent()) {
      paxos_service[i]->update_from_paxos();
    }
  }
}

void Monitor::register_cluster_logger()
{
  if (!cluster_logger_registered) {
    dout(10) << "register_cluster_logger" << dendl;
    cluster_logger_registered = true;
    cct->get_perfcounters_collection()->add(cluster_logger);
  } else {
    dout(10) << "register_cluster_logger - already registered" << dendl;
  }
}

void Monitor::unregister_cluster_logger()
{
  if (cluster_logger_registered) {
    dout(10) << "unregister_cluster_logger" << dendl;
    cluster_logger_registered = false;
    cct->get_perfcounters_collection()->remove(cluster_logger);
  } else {
    dout(10) << "unregister_cluster_logger - not registered" << dendl;
  }
}

void Monitor::update_logger()
{
  cluster_logger->set(l_cluster_num_mon, monmap->size());
  cluster_logger->set(l_cluster_num_mon_quorum, quorum.size());
}

void Monitor::shutdown()
{
  dout(1) << "shutdown" << dendl;
  lock.Lock();

  state = STATE_SHUTDOWN;

  if (admin_hook) {
    AdminSocket* admin_socket = cct->get_admin_socket();
    admin_socket->unregister_command("mon_status");
    admin_socket->unregister_command("quorum_status");
    admin_socket->unregister_command("sync_status");
    delete admin_hook;
    admin_hook = NULL;
  }

  elector.shutdown();

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = NULL;
  }
  if (cluster_logger) {
    if (cluster_logger_registered)
      cct->get_perfcounters_collection()->remove(cluster_logger);
    delete cluster_logger;
    cluster_logger = NULL;
  }
  
  // clean up
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->shutdown();

  timer.shutdown();

  // unlock before msgr shutdown...
  lock.Unlock();

  messenger->shutdown();  // last thing!  ceph_mon.cc will delete mon.
}

void Monitor::bootstrap()
{
  dout(10) << "bootstrap" << dendl;

  unregister_cluster_logger();
  cancel_probe_timeout();

  // note my rank
  int newrank = monmap->get_rank(messenger->get_myaddr());
  if (newrank < 0 && rank >= 0) {
    // was i ever part of the quorum?
    if (has_ever_joined) {
      dout(0) << " removed from monmap, suicide." << dendl;
      exit(0);
    }
  }
  if (newrank != rank) {
    dout(0) << " my rank is now " << newrank << " (was " << rank << ")" << dendl;
    messenger->set_myname(entity_name_t::MON(newrank));
    rank = newrank;

    // reset all connections, or else our peers will think we are someone else.
    messenger->mark_down_all();
  }

  reset_sync();

  // reset
  state = STATE_PROBING;

  reset();

  // singleton monitor?
  if (monmap->size() == 1 && rank == 0) {
    win_standalone_election();
    return;
  }

  reset_probe_timeout();

  // i'm outside the quorum
  if (monmap->contains(name))
    outside_quorum.insert(name);

  // probe monitors
  dout(10) << "probing other monitors" << dendl;
  for (unsigned i = 0; i < monmap->size(); i++) {
    if ((int)i != rank)
      messenger->send_message(new MMonProbe(monmap->fsid, MMonProbe::OP_PROBE, name, has_ever_joined),
			      monmap->get_inst(i));
  }
  for (set<entity_addr_t>::iterator p = extra_probe_peers.begin();
       p != extra_probe_peers.end();
       ++p) {
    if (*p != messenger->get_myaddr()) {
      entity_inst_t i;
      i.name = entity_name_t::MON(-1);
      i.addr = *p;
      messenger->send_message(new MMonProbe(monmap->fsid, MMonProbe::OP_PROBE, name, has_ever_joined), i);
    }
  }
}

void Monitor::_add_bootstrap_peer_hint(string cmd, ostream& ss)
{
  dout(10) << "_add_bootstrap_peer_hint '" << cmd << "'" << dendl;

  if (is_leader() || is_peon()) {
    ss << "mon already active; ignoring bootstrap hint";
    return;
  }

  size_t off = cmd.find(" ");
  if (off == std::string::npos) {
    ss << "syntax is 'add_bootstrap_peer_hint ip[:port]'";
    return;
  }

  entity_addr_t addr;
  const char *end = 0;
  if (!addr.parse(cmd.c_str() + off + 1, &end)) {
    ss << "failed to parse addr '" << (cmd.c_str() + off + 1) << "'";
    return;
  }

  if (addr.get_port() == 0)
    addr.set_port(CEPH_MON_PORT);

  extra_probe_peers.insert(addr);
  ss << "adding peer " << addr << " to list: " << extra_probe_peers;
}

// called by bootstrap(), or on leader|peon -> electing
void Monitor::reset()
{
  dout(10) << "reset" << dendl;
  leader_since = utime_t();
  if (!quorum.empty()) {
    exited_quorum = ceph_clock_now(g_ceph_context);
  }
  quorum.clear();
  outside_quorum.clear();

  paxos->restart();

  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->restart();
}

set<string> Monitor::get_sync_targets_names() {
  set<string> targets;
  targets.insert(paxos->get_name());
  for (int i = 0; i < PAXOS_NUM; ++i)
    targets.insert(paxos_service[i]->get_service_name());

  return targets;
}

/**
 * Reset any lingering sync/trim informations we might have.
 */
void Monitor::reset_sync()
{
  dout(10) << __func__ << dendl;
  // clear everything trim/sync related
  {
    map<entity_inst_t,Context*>::iterator iter = trim_timeouts.begin();
    for (; iter != trim_timeouts.end(); ++iter) {
      if ((*iter).second)
	timer.cancel_event((*iter).second);
    }
    trim_timeouts.clear();
  }
  {
    map<entity_inst_t,SyncEntity>::iterator iter = sync_entities.begin();
    for (; iter != sync_entities.end(); ++iter) {
      (*iter).second->cancel_timeout();
    }
    sync_entities.clear();
  }

  sync_entities_states.clear();

  sync_leader.reset();
  sync_provider.reset();

  sync_state = SYNC_STATE_NONE;
  sync_role = SYNC_ROLE_NONE;
}

// leader

void Monitor::sync_send_heartbeat(entity_inst_t &other, bool reply)
{
  dout(10) << __func__ << " " << other << " reply(" << reply << ")" << dendl;
  uint32_t op = (reply ? MMonSync::OP_HEARTBEAT_REPLY : MMonSync::OP_HEARTBEAT);
  MMonSync *msg = new MMonSync(op);
  messenger->send_message(msg, other);
}

void Monitor::handle_sync_start(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  /* If we are not the leader, then some monitor picked us as the point of
   * entry to the quorum during its synchronization process. Therefore, we
   * have an obligation of forwarding this message to leader, so the sender
   * can start synchronizing.
   */
  if (!is_leader() && (quorum.size() > 0)) {
    entity_inst_t leader = monmap->get_inst(get_leader());
    MMonSync *msg = new MMonSync(m);
    msg->reply_to = m->get_source_inst();
    msg->flags |= MMonSync::FLAG_REPLY_TO;
    dout(10) << __func__ << " forward " << *m
	     << " to leader at " << leader << dendl;
    assert(g_conf->mon_sync_provider_kill_at != 1);
    messenger->send_message(msg, leader);
    assert(g_conf->mon_sync_provider_kill_at != 2);
    return;
  }

  Mutex::Locker l(trim_lock);
  entity_inst_t other =
    (m->flags & MMonSync::FLAG_REPLY_TO ? m->reply_to : m->get_source_inst());

  assert(g_conf->mon_sync_leader_kill_at != 1);

  if (trim_timeouts.count(other) > 0) {
    dout(1) << __func__ << " sync session already in progress for " << other
	    << dendl;

    if (sync_entities_states[other] != SYNC_STATE_NONE) {
      dout(1) << __func__ << "    ignore stray message" << dendl;
      m->put();
      return;
    }

    dout(1) << __func__<< "    destroying current state and creating new"
	    << dendl;

    if (trim_timeouts[other])
      timer.cancel_event(trim_timeouts[other]);
    trim_timeouts.erase(other);
    sync_entities_states.erase(other);
  }

  MMonSync *msg = new MMonSync(MMonSync::OP_START_REPLY);

  if (((quorum.size() > 0) && paxos->should_trim())
      || (trim_enable_timer != NULL)) {
    msg->flags |= MMonSync::FLAG_RETRY;
  } else {
    trim_timeouts.insert(make_pair(other, new C_TrimTimeout(this, other)));
    timer.add_event_after(g_conf->mon_sync_trim_timeout, trim_timeouts[other]);

    sync_entities_states[other] = SYNC_STATE_START;
    sync_role |= SYNC_ROLE_LEADER;

    paxos->trim_disable();
  }
  messenger->send_message(msg, other);
  m->put();

  assert(g_conf->mon_sync_leader_kill_at != 2);
}

void Monitor::handle_sync_heartbeat(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();
  if (!(sync_role & SYNC_ROLE_LEADER)
      || !sync_entities_states.count(other)
      || (sync_entities_states[other] != SYNC_STATE_START)) {
    // stray message; ignore.
    dout(1) << __func__ << " ignored stray message " << *m << dendl;
    m->put();
    return;
  }

  if (!is_leader() && (quorum.size() > 0)
      && (trim_timeouts.count(other) > 0)) {
    // we must have been the leader before, but we lost leadership to
    // someone else.
    sync_finish_abort(other);
    m->put();
    return;
  }

  assert(trim_timeouts.count(other) > 0);

  if (trim_timeouts[other])
    timer.cancel_event(trim_timeouts[other]);
  trim_timeouts[other] = new C_TrimTimeout(this, other);
  timer.add_event_after(g_conf->mon_sync_trim_timeout, trim_timeouts[other]);

  assert(g_conf->mon_sync_leader_kill_at != 3);
  sync_send_heartbeat(other, true);
  assert(g_conf->mon_sync_leader_kill_at != 4);

  m->put();
}

void Monitor::sync_finish(entity_inst_t &entity, bool abort)
{
  dout(10) << __func__ << " entity(" << entity << ")" << dendl;

  Mutex::Locker l(trim_lock);

  if (!trim_timeouts.count(entity)) {
    dout(1) << __func__ << " we know of no sync effort from "
	    << entity << " -- ignore it." << dendl;
    return;
  }

  if (trim_timeouts[entity] != NULL)
    timer.cancel_event(trim_timeouts[entity]);

  trim_timeouts.erase(entity);
  sync_entities_states.erase(entity);

  if (abort) {
    MMonSync *m = new MMonSync(MMonSync::OP_ABORT);
    assert(g_conf->mon_sync_leader_kill_at != 5);
    messenger->send_message(m, entity);
    assert(g_conf->mon_sync_leader_kill_at != 6);
  }

  if (trim_timeouts.size() > 0)
    return;

  dout(10) << __func__ << " no longer a sync leader" << dendl;
  sync_role &= ~SYNC_ROLE_LEADER;

  // we may have been the leader, but by now we may no longer be.
  // this can happen when the we sync'ed a monitor that became the
  // leader, or that same monitor simply came back to life and got
  // elected as the new leader.
  if (is_leader() && paxos->is_trim_disabled()) {
    trim_enable_timer = new C_TrimEnable(this);
    timer.add_event_after(30.0, trim_enable_timer);
  }
}

void Monitor::handle_sync_finish(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();

  if (!trim_timeouts.count(other) || !sync_entities_states.count(other)
      || (sync_entities_states[other] != SYNC_STATE_START)) {
    dout(1) << __func__ << " ignored stray message from " << other << dendl;
    m->put();
    return;
  }

  // We may no longer the leader. In such case, we should just inform the
  // other monitor that he should abort his sync. However, it appears that
  // his sync has finished, so there is no use in scraping the whole thing
  // now. Therefore, just go along and acknowledge.
  if (!is_leader()) {
    dout(10) << __func__ << " We are no longer the leader; reply nonetheless"
	     << dendl;
  }

  MMonSync *msg = new MMonSync(MMonSync::OP_FINISH_REPLY);
  assert(g_conf->mon_sync_leader_kill_at != 7);
  messenger->send_message(msg, other);
  assert(g_conf->mon_sync_leader_kill_at != 8);

  sync_finish(other);
  m->put();
}

// end of leader

// synchronization provider

void Monitor::sync_timeout(entity_inst_t &entity)
{
  if (state == STATE_SYNCHRONIZING) {
    assert(sync_role == SYNC_ROLE_REQUESTER);
    assert(sync_state == SYNC_STATE_CHUNKS);

    // we are a sync requester; our provider just timed out, so find another
    // monitor to synchronize with.
    dout(1) << __func__ << " " << sync_provider->entity << dendl;

    sync_provider->attempts++;
    if ((sync_provider->attempts > g_conf->mon_sync_max_retries)
	|| (monmap->size() == 2)) {
      // We either tried too many times to sync, or there's just us and the
      // monitor we were attempting to sync with.
      // Therefore, just abort the whole sync and start off fresh whenever he
      // (or somebody else) comes back.
      sync_requester_abort();
      return;
    }

    int i = 0;
    string entity_name = monmap->get_name(entity.addr);
    string debug_mon = g_conf->mon_sync_debug_provider;
    string debug_fallback = g_conf->mon_sync_debug_provider_fallback;
    while ((i++) < 2*monmap->size()) {
      // we are trying to pick a random monitor, but we cannot do this forever.
      // in case something goes awfully wrong, just stop doing it after a
      // couple of attempts and try again later.
      string new_mon = monmap->pick_random_mon();

      if (!debug_fallback.empty()) {
	if (entity_name != debug_fallback)
	  new_mon = debug_fallback;
	else if (!debug_mon.empty() && (entity_name != debug_mon))
	  new_mon = debug_mon;
      }

      if ((new_mon != name) && (new_mon != entity_name)) {
	sync_provider->entity = monmap->get_inst(new_mon);
	sync_state = SYNC_STATE_START;
	sync_start_chunks(sync_provider);
	return;
      }
    }

    assert(0 == "Unable to find a new monitor to connect to. Not cool.");
  } else if (sync_role & SYNC_ROLE_PROVIDER) {
    dout(10) << __func__ << " cleanup " << entity << dendl;
    sync_provider_cleanup(entity);
    return;
  } else
    assert(0 == "We should never reach this");
}

void Monitor::sync_provider_cleanup(entity_inst_t &entity)
{
  dout(10) << __func__ << " " << entity << dendl;
  if (sync_entities.count(entity) > 0) {
    sync_entities[entity]->cancel_timeout();
    sync_entities.erase(entity);
    sync_entities_states.erase(entity);
  }

  if (sync_entities.size() == 0) {
    dout(1) << __func__ << " no longer a sync provider" << dendl;
    sync_role &= ~SYNC_ROLE_PROVIDER;
  }
}

void Monitor::handle_sync_start_chunks(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();

  // if we have a sync going on for this entity, just drop the message. If it
  // was a stray message, we did the right thing. If it wasn't, then it means
  // that we still have an old state of this entity, and that the said entity
  // failed in the meantime and is now up again; therefore, just let the
  // timeout timers fulfill their purpose and deal with state cleanup when
  // they are triggered. Until then, no Sir, we won't accept your messages.
  if (sync_entities.count(other) > 0) {
    dout(1) << __func__ << " sync session already in progress for " << other
	    << " -- assumed as stray message." << dendl;
    m->put();
    return;
  }

  SyncEntity sync = get_sync_entity(other, this);
  sync->version = paxos->get_version();

  if (!m->last_key.first.empty() && !m->last_key.second.empty()) {
    sync->last_received_key = m->last_key;
    dout(10) << __func__ << " set last received key to ("
	     << sync->last_received_key.first << ","
	     << sync->last_received_key.second << ")" << dendl;
  }

  sync->sync_init();

  sync_entities.insert(make_pair(other, sync));
  sync_entities_states[other] = SYNC_STATE_START;
  sync_role |= SYNC_ROLE_PROVIDER;

  sync_send_chunks(sync);
  m->put();
}

void Monitor::handle_sync_chunk_reply(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();

  if (!(sync_role & SYNC_ROLE_PROVIDER)
      || !sync_entities.count(other)
      || (sync_entities_states[other] != SYNC_STATE_START)) {
    dout(1) << __func__ << " ignored stray message from " << other << dendl;
    m->put();
    return;
  }

  if (m->flags & MMonSync::FLAG_LAST) {
    // they acked the last chunk. Clean up.
    sync_provider_cleanup(other);
    m->put();
    return;
  }

  sync_send_chunks(sync_entities[other]);
  m->put();
}

void Monitor::sync_send_chunks(SyncEntity sync)
{
  dout(10) << __func__ << " entity(" << sync->entity << ")" << dendl;

  sync->cancel_timeout();

  assert(sync->synchronizer.use_count() > 0);
  assert(sync->synchronizer->has_next_chunk());

  MMonSync *msg = new MMonSync(MMonSync::OP_CHUNK);

  sync->synchronizer->get_chunk(msg->chunk_bl);
  msg->last_key = sync->synchronizer->get_last_key();
  dout(10) << __func__ << " last key ("
	   << msg->last_key.first << ","
	   << msg->last_key.second << ")" << dendl;

  sync->sync_update();

  if (sync->has_crc()) {
    msg->flags |= MMonSync::FLAG_CRC;
    msg->crc = sync->crc_get();
    sync->crc_clear();
  }

  if (!sync->synchronizer->has_next_chunk()) {
    msg->flags |= MMonSync::FLAG_LAST;
    sync->synchronizer.reset();
  }

  sync->set_timeout(new C_SyncTimeout(this, sync->entity),
		    g_conf->mon_sync_timeout);
  assert(g_conf->mon_sync_provider_kill_at != 3);
  messenger->send_message(msg, sync->entity);
  assert(g_conf->mon_sync_provider_kill_at != 4);

  // kill the monitor as soon as we move into synchronizing the paxos versions.
  // This is intended as debug.
  if (sync->sync_state == SyncEntityImpl::STATE_PAXOS)
    assert(g_conf->mon_sync_provider_kill_at != 5);


}
// end of synchronization provider

// start of synchronization requester

void Monitor::sync_requester_abort()
{
  dout(10) << __func__;
  assert(state == STATE_SYNCHRONIZING);
  assert(sync_role == SYNC_ROLE_REQUESTER);

  if (sync_leader.get() != NULL) {
    *_dout << " " << sync_leader->entity;
    sync_leader->cancel_timeout();
    sync_leader.reset();
  }

  if (sync_provider.get() != NULL) {
    *_dout << " " << sync_provider->entity;
    sync_provider->cancel_timeout();

    MMonSync *msg = new MMonSync(MMonSync::OP_ABORT);
    messenger->send_message(msg, sync_provider->entity);

    sync_provider.reset();
  }
  *_dout << " clearing potentially inconsistent store" << dendl;

  // Given that we are explicitely aborting the whole sync process, we should
  // play it safe and clear the store.
  set<string> targets = get_sync_targets_names();
  targets.insert("mon_sync");
  store->clear(targets);

  dout(1) << __func__ << " no longer a sync requester" << dendl;
  sync_role = SYNC_ROLE_NONE;
  sync_state = SYNC_STATE_NONE;

  state = 0;

  bootstrap();
}

/**
 * Start Sync process
 *
 * Create SyncEntity instances for the leader and the provider;
 * Send OP_START message to the leader;
 * Set trim timeout on the leader
 *
 * @param other Synchronization provider to-be.
 */
void Monitor::sync_start(entity_inst_t &other)
{
  cancel_probe_timeout();

  dout(10) << __func__ << " entity( " << other << " )" << dendl;
  if ((state == STATE_SYNCHRONIZING) && (sync_role == SYNC_ROLE_REQUESTER)) {
    dout(1) << __func__ << " already synchronizing; drop it" << dendl;
    return;
  }

  assert(sync_role == SYNC_ROLE_NONE);
  assert(sync_state == SYNC_STATE_NONE);

  state = STATE_SYNCHRONIZING;
  sync_role = SYNC_ROLE_REQUESTER;
  sync_state = SYNC_STATE_START;

  // clear the underlying store, since we are starting a whole
  // sync process from the bare beginning.
  set<string> targets = get_sync_targets_names();
  targets.insert("mon_sync");
  store->clear(targets);

  MonitorDBStore::Transaction t;
  t.put("mon_sync", "in_sync", 1);
  store->apply_transaction(t);

  // assume 'other' as the leader. We will update the leader once we receive
  // a reply to the sync start.
  entity_inst_t leader = other;
  entity_inst_t provider = other;

  if (!g_conf->mon_sync_debug_leader.empty()) {
    leader = monmap->get_inst(g_conf->mon_sync_debug_leader);
    dout(10) << __func__ << " assuming " << leader
	     << " as the leader for debug" << dendl;
  }

  if (!g_conf->mon_sync_debug_provider.empty()) {
    provider = monmap->get_inst(g_conf->mon_sync_debug_provider);
    dout(10) << __func__ << " assuming " << provider
	     << " as the provider for debug" << dendl;
  }

  sync_leader = get_sync_entity(leader, this);
  sync_provider = get_sync_entity(provider, this);

  // this message may bounce through 'other' (if 'other' is not the leader)
  // in order to reach the leader. Therefore, set a higher timeout to allow
  // breathing room for the reply message to reach us.
  sync_leader->set_timeout(new C_SyncStartTimeout(this),
			   g_conf->mon_sync_trim_timeout*2);

  MMonSync *m = new MMonSync(MMonSync::OP_START);
  messenger->send_message(m, other);
  assert(g_conf->mon_sync_requester_kill_at != 1);
}

void Monitor::sync_start_chunks(SyncEntity provider)
{
  dout(10) << __func__ << " provider(" << provider->entity << ")" << dendl;

  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_START);

  sync_state = SYNC_STATE_CHUNKS;

  provider->set_timeout(new C_SyncTimeout(this, provider->entity),
			g_conf->mon_sync_timeout);
  MMonSync *msg = new MMonSync(MMonSync::OP_START_CHUNKS);
  pair<string,string> last_key = provider->last_received_key;
  if (!last_key.first.empty() && !last_key.second.empty())
    msg->last_key = last_key;

  assert(g_conf->mon_sync_requester_kill_at != 4);
  messenger->send_message(msg, provider->entity);
  assert(g_conf->mon_sync_requester_kill_at != 5);
}

void Monitor::sync_start_reply_timeout()
{
  dout(10) << __func__ << dendl;

  assert(state == STATE_SYNCHRONIZING);
  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_START);

  // Restart the sync attempt. It's not as if we were going to lose a vast
  // amount of work, and if we take into account that we are timing out while
  // waiting for a reply from the Leader, it sure seems like the right path
  // to take.
  sync_requester_abort();
}

void Monitor::handle_sync_start_reply(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();

  if ((sync_role != SYNC_ROLE_REQUESTER)
      || (sync_state != SYNC_STATE_START)) {
    // If the leader has sent this message before we failed, there is no point
    // in replying to it, as he has no idea that we actually received it. On
    // the other hand, if he received one of our stray messages (because it was
    // delivered once he got back up after failing) and replied accordingly,
    // there is a chance that he did stopped trimming on our behalf. However,
    // we have no way to know it, and we really don't want to mess with his
    // state if that is not the case. Therefore, just drop it and let the
    // timeouts figure it out. Eventually.
    dout(1) << __func__ << " stray message -- drop it." << dendl;
    m->put();
    return;
  }

  assert(state == STATE_SYNCHRONIZING);
  assert(sync_leader.get() != NULL);
  assert(sync_provider.get() != NULL);

  // We now know for sure who the leader is.
  sync_leader->entity = other;
  sync_leader->cancel_timeout();
  
  if (m->flags & MMonSync::FLAG_RETRY) {
    dout(10) << __func__ << " retrying sync at a later time" << dendl;
    sync_role = SYNC_ROLE_NONE;
    sync_state = SYNC_STATE_NONE;
    sync_leader->set_timeout(new C_SyncStartRetry(this, sync_leader->entity),
			     g_conf->mon_sync_backoff_timeout);
    return;
  }

  sync_leader->set_timeout(new C_HeartbeatTimeout(this),
			   g_conf->mon_sync_heartbeat_timeout);

  assert(g_conf->mon_sync_requester_kill_at != 2);
  sync_send_heartbeat(sync_leader->entity);
  assert(g_conf->mon_sync_requester_kill_at != 3);

  sync_start_chunks(sync_provider);
}

void Monitor::handle_sync_heartbeat_reply(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();
  if ((sync_role != SYNC_ROLE_REQUESTER)
      || (sync_state == SYNC_STATE_NONE)
      || (sync_leader.get() == NULL)
      || (other != sync_leader->entity)) {
    dout(1) << __func__ << " stray message -- drop it." << dendl;
    m->put();
    return;
  }

  assert(state == STATE_SYNCHRONIZING);
  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state != SYNC_STATE_NONE);

  assert(sync_leader.get() != NULL);
  assert(sync_leader->entity == other);

  sync_leader->cancel_timeout();
  sync_leader->set_timeout(new C_HeartbeatInterval(this, sync_leader->entity),
			   g_conf->mon_sync_heartbeat_interval);
}

void Monitor::handle_sync_chunk(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();

  if ((sync_role != SYNC_ROLE_REQUESTER)
      || (sync_state != SYNC_STATE_CHUNKS)
      || (sync_provider.get() == NULL)
      || (other != sync_provider->entity)) {
    dout(1) << __func__ << " stray message -- drop it." << dendl;
    m->put();
    return;
  }

  assert(state == STATE_SYNCHRONIZING);
  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_CHUNKS);

  assert(sync_leader.get() != NULL);

  assert(sync_provider.get() != NULL);
  assert(other == sync_provider->entity);

  sync_provider->cancel_timeout();

  MonitorDBStore::Transaction tx;
  tx.append_from_encoded(m->chunk_bl);

  sync_provider->set_timeout(new C_SyncTimeout(this, sync_provider->entity),
			     g_conf->mon_sync_timeout);
  sync_provider->last_received_key = m->last_key;

  MMonSync *msg = new MMonSync(MMonSync::OP_CHUNK_REPLY);

  bool stop = false;
  if (m->flags & MMonSync::FLAG_LAST) {
    msg->flags |= MMonSync::FLAG_LAST;
    stop = true;
  }
  assert(g_conf->mon_sync_requester_kill_at != 8);
  messenger->send_message(msg, sync_provider->entity);

  store->apply_transaction(tx);

  if (g_conf->mon_sync_debug && (m->flags & MMonSync::FLAG_CRC)) {
    dout(10) << __func__ << " checking CRC" << dendl;
    MonitorDBStore::Synchronizer sync;
    if (m->flags & MMonSync::FLAG_LAST) {
      dout(10) << __func__ << " checking CRC only for Paxos" << dendl;
      string paxos_name("paxos");
      sync = store->get_synchronizer(paxos_name);
    } else {
      dout(10) << __func__ << " checking CRC for all prefixes" << dendl;
      set<string> prefixes = get_sync_targets_names();
      pair<string,string> empty_key;
      sync = store->get_synchronizer(empty_key, prefixes);
    }

    while (sync->has_next_chunk()) {
      bufferlist bl;
      sync->get_chunk(bl);
    }
    __u32 got_crc = sync->crc();
    dout(10) << __func__ << " expected crc " << m->crc
	     << " got " << got_crc << dendl;

    assert(m->crc == got_crc);
    dout(10) << __func__ << " CRC matches" << dendl;
  }

  m->put();
  if (stop)
    sync_stop();
}

void Monitor::sync_stop()
{
  dout(10) << __func__ << dendl;

  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_CHUNKS);

  sync_state = SYNC_STATE_STOP;

  sync_leader->cancel_timeout();
  sync_provider->cancel_timeout();
  sync_provider.reset();

  entity_inst_t leader = sync_leader->entity;

  sync_leader->set_timeout(new C_SyncFinishReplyTimeout(this),
			   g_conf->mon_sync_timeout);

  MMonSync *msg = new MMonSync(MMonSync::OP_FINISH);
  assert(g_conf->mon_sync_requester_kill_at != 9);
  messenger->send_message(msg, leader);
  assert(g_conf->mon_sync_requester_kill_at != 10);
}

void Monitor::sync_finish_reply_timeout()
{
  dout(10) << __func__ << dendl;
  assert(state == STATE_SYNCHRONIZING);
  assert(sync_leader.get() != NULL);
  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_STOP);

  sync_requester_abort();
}

void Monitor::handle_sync_finish_reply(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  entity_inst_t other = m->get_source_inst();

  if ((sync_role != SYNC_ROLE_REQUESTER)
      || (sync_state != SYNC_STATE_STOP)
      || (sync_leader.get() == NULL)
      || (sync_leader->entity != other)) {
    dout(1) << __func__ << " stray message -- drop it." << dendl;
    m->put();
    return;
  }

  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_STOP);

  assert(sync_leader.get() != NULL);
  assert(sync_leader->entity == other);

  sync_role = SYNC_ROLE_NONE;
  sync_state = SYNC_STATE_NONE;

  sync_leader->cancel_timeout();
  sync_leader.reset();


  MonitorDBStore::Transaction t;
  t.erase("mon_sync", "in_sync");
  store->apply_transaction(t);

  init_paxos();

  assert(g_conf->mon_sync_requester_kill_at != 11);

  bootstrap();
}

void Monitor::handle_sync_abort(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  /* This function's responsabilities are manifold, and they depend on
   * who we (the monitor) are and what is our role in the sync.
   *
   * If we are the sync requester (i.e., if we are synchronizing), it
   * means that we *must* abort the current sync and bootstrap. This may
   * be required if there was a leader change and we are talking to the
   * wrong leader, which makes continuing with the current sync way too
   * risky, given that a Paxos trim may be underway and we certainly incur
   * in the chance of ending up with an inconsistent store state.
   *
   * If we are the sync provider, it means that the requester wants to
   * abort his sync, either because he lost connectivity to the leader
   * (i.e., his heartbeat timeout was triggered) or he became aware of a
   * leader change.
   *
   * As a leader, we should never receive such a message though, unless we
   * have just won an election, in which case we should have been a sync
   * provider before. In such a case, we should behave as if we were a sync
   * provider and clean up the requester's state.
   */
  entity_inst_t other = m->get_source_inst();

  if ((sync_role == SYNC_ROLE_REQUESTER)
      && (sync_leader.get() != NULL)
      && (sync_leader->entity == other)) {

    sync_requester_abort();
  } else if ((sync_role & SYNC_ROLE_PROVIDER)
	     && (sync_entities.count(other) > 0)
	     && (sync_entities_states[other] == SYNC_STATE_START)) {

    sync_provider_cleanup(other);
  } else {
    dout(1) << __func__ << " stray message -- drop it." << dendl;
  }
  m->put();
}

void Monitor::handle_sync(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  switch (m->op) {
  case MMonSync::OP_START:
    handle_sync_start(m);
    break;
  case MMonSync::OP_START_REPLY:
    handle_sync_start_reply(m);
    break;
  case MMonSync::OP_HEARTBEAT:
    handle_sync_heartbeat(m);
    break;
  case MMonSync::OP_HEARTBEAT_REPLY:
    handle_sync_heartbeat_reply(m);
    break;
  case MMonSync::OP_FINISH:
    handle_sync_finish(m);
    break;
  case MMonSync::OP_START_CHUNKS:
    handle_sync_start_chunks(m);
    break;
  case MMonSync::OP_CHUNK:
    handle_sync_chunk(m);
    break;
  case MMonSync::OP_CHUNK_REPLY:
    handle_sync_chunk_reply(m);
    break;
  case MMonSync::OP_FINISH_REPLY:
    handle_sync_finish_reply(m);
    break;
  case MMonSync::OP_ABORT:
    handle_sync_abort(m);
    break;
  default:
    dout(0) << __func__ << " unknown op " << m->op << dendl;
    assert(0);
    break;
  }
}

void Monitor::cancel_probe_timeout()
{
  if (probe_timeout_event) {
    dout(10) << "cancel_probe_timeout " << probe_timeout_event << dendl;
    timer.cancel_event(probe_timeout_event);
    probe_timeout_event = NULL;
  } else {
    dout(10) << "cancel_probe_timeout (none scheduled)" << dendl;
  }
}

void Monitor::reset_probe_timeout()
{
  cancel_probe_timeout();
  probe_timeout_event = new C_ProbeTimeout(this);
  double t = g_conf->mon_probe_timeout;
  timer.add_event_after(t, probe_timeout_event);
  dout(10) << "reset_probe_timeout " << probe_timeout_event << " after " << t << " seconds" << dendl;
}

void Monitor::probe_timeout(int r)
{
  dout(4) << "probe_timeout " << probe_timeout_event << dendl;
  assert(is_probing() || is_synchronizing());
  assert(probe_timeout_event);
  probe_timeout_event = NULL;
  bootstrap();
}

void Monitor::handle_probe(MMonProbe *m)
{
  dout(10) << "handle_probe " << *m << dendl;

  if (m->fsid != monmap->fsid) {
    dout(0) << "handle_probe ignoring fsid " << m->fsid << " != " << monmap->fsid << dendl;
    m->put();
    return;
  }

  switch (m->op) {
  case MMonProbe::OP_PROBE:
    handle_probe_probe(m);
    break;

  case MMonProbe::OP_REPLY:
    handle_probe_reply(m);
    break;

  default:
    m->put();
  }
}

/**
 * @todo fix this. This is going to cause trouble.
 */
void Monitor::handle_probe_probe(MMonProbe *m)
{
  dout(10) << "handle_probe_probe " << m->get_source_inst() << *m << dendl;
  MMonProbe *r = new MMonProbe(monmap->fsid, MMonProbe::OP_REPLY, name, has_ever_joined);
  r->name = name;
  r->quorum = quorum;
  monmap->encode(r->monmap_bl, m->get_connection()->get_features());
  r->paxos_first_version = paxos->get_first_committed();
  r->paxos_last_version = paxos->get_version();
  messenger->send_message(r, m->get_connection());

  // did we discover a peer here?
  if (!monmap->contains(m->get_source_addr())) {
    dout(1) << " adding peer " << m->get_source_addr() << " to list of hints" << dendl;
    extra_probe_peers.insert(m->get_source_addr());
  }
  m->put();
}

void Monitor::handle_probe_reply(MMonProbe *m)
{
  dout(10) << "handle_probe_reply " << m->get_source_inst() << *m << dendl;
  dout(10) << " monmap is " << *monmap << dendl;

  if (!is_probing()) {
    m->put();
    return;
  }

  // newer map, or they've joined a quorum and we haven't?
  bufferlist mybl;
  monmap->encode(mybl, m->get_connection()->get_features());
  // make sure it's actually different; the checks below err toward
  // taking the other guy's map, which could cause us to loop.
  if (!mybl.contents_equal(m->monmap_bl)) {
    MonMap *newmap = new MonMap;
    newmap->decode(m->monmap_bl);
    if (m->has_ever_joined && (newmap->get_epoch() > monmap->get_epoch() ||
			       !has_ever_joined)) {
      dout(10) << " got newer/committed monmap epoch " << newmap->get_epoch()
	       << ", mine was " << monmap->get_epoch() << dendl;
      delete newmap;
      monmap->decode(m->monmap_bl);
      m->put();

      bootstrap();
      return;
    }
    delete newmap;
  }

  // rename peer?
  string peer_name = monmap->get_name(m->get_source_addr());
  if (monmap->get_epoch() == 0 && peer_name.find("noname-") == 0) {
    dout(10) << " renaming peer " << m->get_source_addr() << " "
	     << peer_name << " -> " << m->name << " in my monmap"
	     << dendl;
    monmap->rename(peer_name, m->name);
  } else {
    dout(10) << " peer name is " << peer_name << dendl;
  }

  // new initial peer?
  if (monmap->contains(m->name)) {
    if (monmap->get_addr(m->name).is_blank_ip()) {
      dout(1) << " learned initial mon " << m->name << " addr " << m->get_source_addr() << dendl;
      monmap->set_addr(m->name, m->get_source_addr());
      m->put();

      bootstrap();
      return;
    }
  }

  // is there an existing quorum?
  if (m->quorum.size()) {
    dout(10) << " existing quorum " << m->quorum << dendl;

    assert(paxos != NULL);
    // do i need to catch up?
    bool ok = true;
    if (is_synchronizing()) {
      dout(10) << "We are currently synchronizing, so that will continue."
	<< " Peer has versions [" << m->paxos_first_version
	<< "," << m->paxos_last_version << "]" << dendl;
      m->put();
      return;
    } else if (paxos->get_version() + g_conf->paxos_max_join_drift < m->paxos_last_version) {
      dout(10) << " peer paxos version " << m->paxos_last_version
	       << " vs my version " << paxos->get_version()
	       << " (too far ahead)"
	       << dendl;
      ok = false;
    } else {
      dout(10) << " peer paxos version " << m->paxos_last_version
	<< " vs my version " << paxos->get_version()
	<< " (ok)"
	<< dendl;
    }
    if (ok) {
      if (monmap->contains(name) &&
	  !monmap->get_addr(name).is_blank_ip()) {
	// i'm part of the cluster; just initiate a new election
	start_election();
      } else {
	dout(10) << " ready to join, but i'm not in the monmap or my addr is blank, trying to join" << dendl;
	messenger->send_message(new MMonJoin(monmap->fsid, name, messenger->get_myaddr()),
				monmap->get_inst(*m->quorum.begin()));
      }
    } else {
      entity_inst_t source = m->get_source_inst();
      sync_start(source);
    }
  } else {
    // check if our store is enough up-to-date so that forming a quorum
    // actually works. Otherwise, we'd be entering a world of pain and
    // out-of-date states -- this can happen, for instance, if only one
    // mon is up, and we are starting fresh.
    entity_inst_t other = m->get_source_inst();
    if (m->paxos_first_version > paxos->get_version()) {
      sync_start(other);
    } else if (paxos->get_first_committed() > m->paxos_last_version) {
      dout(10) << __func__ << " waiting for " << other
	<< " to sync with us (our fc: "
	<< paxos->get_first_committed() << "; theirs lc: "
	<< m->paxos_last_version << ")" << dendl;
    } else {
      // not part of a quorum
      if (monmap->contains(m->name))
	outside_quorum.insert(m->name);
      else
	dout(10) << " mostly ignoring mon." << m->name << ", not part of monmap" << dendl;

      unsigned need = monmap->size() / 2 + 1;
      dout(10) << " outside_quorum now " << outside_quorum << ", need " << need << dendl;

      if (outside_quorum.size() >= need) {
	if (outside_quorum.count(name)) {
	  dout(10) << " that's enough to form a new quorum, calling election" << dendl;
	  start_election();
	} else {
	  dout(10) << " that's enough to form a new quorum, but it does not include me; waiting" << dendl;
	}
      } else {
	dout(10) << " that's not yet enough for a new quorum, waiting" << dendl;
      }
    }
  }
  m->put();
}

void Monitor::start_election()
{
  dout(10) << "start_election" << dendl;

  cancel_probe_timeout();

  // call a new election
  state = STATE_ELECTING;
  clog.info() << "mon." << name << " calling new monitor election\n";
  elector.call_election();
}

void Monitor::win_standalone_election()
{
  dout(1) << "win_standalone_election" << dendl;
  rank = monmap->get_rank(name);
  assert(rank == 0);
  set<int> q;
  q.insert(rank);
  win_election(1, q);
}

const utime_t& Monitor::get_leader_since() const
{
  assert(state == STATE_LEADER);
  return leader_since;
}

epoch_t Monitor::get_epoch()
{
  return elector.get_epoch();
}

void Monitor::win_election(epoch_t epoch, set<int>& active) 
{
  if (!is_electing())
    reset();

  state = STATE_LEADER;
  leader_since = ceph_clock_now(g_ceph_context);
  leader = rank;
  quorum = active;
  outside_quorum.clear();
  dout(10) << "win_election, epoch " << epoch << " quorum is " << quorum << dendl;

  clog.info() << "mon." << name << "@" << rank
		<< " won leader election with quorum " << quorum << "\n";
 
  paxos->leader_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_finished();

  finish_election();
}

void Monitor::lose_election(epoch_t epoch, set<int> &q, int l) 
{
  state = STATE_PEON;
  leader_since = utime_t();
  leader = l;
  quorum = q;
  outside_quorum.clear();
  dout(10) << "lose_election, epoch " << epoch << " leader is mon" << leader
	   << " quorum is " << quorum << dendl;

  // let everyone currently syncing know that we are no longer the leader and
  // that they should all abort their on-going syncs
  for (map<entity_inst_t,Context*>::iterator iter = trim_timeouts.begin();
       iter != trim_timeouts.end();
       ++iter) {
    timer.cancel_event((*iter).second);
    entity_inst_t entity = (*iter).first;
    MMonSync *msg = new MMonSync(MMonSync::OP_ABORT);
    messenger->send_message(msg, entity);
  }
  trim_timeouts.clear();
  sync_role &= ~SYNC_ROLE_LEADER;
  
  paxos->peon_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_finished();

  finish_election();
}

void Monitor::finish_election()
{
  exited_quorum = utime_t();
  finish_contexts(g_ceph_context, waitfor_quorum);
  finish_contexts(g_ceph_context, maybe_wait_for_quorum);
  resend_routed_requests();
  update_logger();
  register_cluster_logger();

  // am i named properly?
  string cur_name = monmap->get_name(messenger->get_myaddr());
  if (cur_name != name) {
    dout(10) << " renaming myself from " << cur_name << " -> " << name << dendl;
    messenger->send_message(new MMonJoin(monmap->fsid, name, messenger->get_myaddr()),
			    monmap->get_inst(*quorum.begin()));
  }
} 


bool Monitor::_allowed_command(MonSession *s, const vector<string>& cmd)
{
  for (list<list<string> >::iterator p = s->caps.cmd_allow.begin();
       p != s->caps.cmd_allow.end();
       ++p) {
    list<string>::iterator q;
    unsigned i;
    dout(0) << "cmd " << cmd << " vs " << *p << dendl;
    for (q = p->begin(), i = 0; q != p->end() && i < cmd.size(); ++q, ++i) {
      if (*q == "*")
	continue;
      if (*q == "...") {
	i = cmd.size() - 1;
	continue;
      }	
      if (*q != cmd[i])
	break;
    }
    if (q == p->end() && i == cmd.size())
      return true;   // match
  }

  return false;
}

void Monitor::_sync_status(ostream& ss)
{
  JSONFormatter jf(true);
  jf.open_object_section("sync_status");
  jf.dump_string("state", get_state_name());
  jf.dump_unsigned("paxos_version", paxos->get_version());

  if (is_leader() || (sync_role == SYNC_ROLE_LEADER)) {
    Mutex::Locker l(trim_lock);
    jf.open_object_section("trim");
    jf.dump_int("disabled", paxos->is_trim_disabled());
    jf.dump_int("should_trim", paxos->should_trim());
    if (trim_timeouts.size() > 0) {
      jf.open_array_section("mons");
      for (map<entity_inst_t,Context*>::iterator it = trim_timeouts.begin();
	   it != trim_timeouts.end();
	   ++it) {
	jf.dump_stream("mon") << (*it).first;
      }
    }
    jf.close_section();
  }

  if ((sync_entities.size() > 0) || (sync_role == SYNC_ROLE_PROVIDER)) {
    jf.open_array_section("on_going");
    for (map<entity_inst_t,SyncEntity>::iterator it = sync_entities.begin();
	 it != sync_entities.end();
	 ++it) {
      jf.open_object_section("mon");
      jf.dump_stream("addr") << (*it).first;
      jf.dump_string("state", (*it).second->get_state());
      jf.close_section();
    }
    jf.close_section();
  }

  if (is_synchronizing() || (sync_role == SYNC_ROLE_REQUESTER)) {
    jf.open_object_section("leader");
    SyncEntity sync_entity = sync_leader;
    if (sync_entity.get() != NULL)
      jf.dump_stream("addr") << sync_entity->entity;
    jf.close_section();

    jf.open_object_section("provider");
    sync_entity = sync_provider;
    if (sync_entity.get() != NULL)
      jf.dump_stream("addr") << sync_entity->entity;
    jf.close_section();
  }

  if (g_conf->mon_sync_leader_kill_at > 0)
    jf.dump_int("leader_kill_at", g_conf->mon_sync_leader_kill_at);
  if (g_conf->mon_sync_provider_kill_at > 0)
    jf.dump_int("provider_kill_at", g_conf->mon_sync_provider_kill_at);
  if (g_conf->mon_sync_requester_kill_at > 0)
    jf.dump_int("requester_kill_at", g_conf->mon_sync_requester_kill_at);

  jf.close_section();
  jf.flush(ss);
}

void Monitor::_sync_force(ostream& ss)
{
  MonitorDBStore::Transaction tx;
  tx.put("mon_sync", "force_sync", 1);
  store->apply_transaction(tx);

  ss << "forcing store sync the next time the monitor starts";
}

void Monitor::_quorum_status(ostream& ss)
{
  JSONFormatter jf(true);
  jf.open_object_section("quorum_status");
  jf.dump_int("election_epoch", get_epoch());
  
  jf.open_array_section("quorum");
  for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
    jf.dump_int("mon", *p);
  jf.close_section();

  jf.open_object_section("monmap");
  monmap->dump(&jf);
  jf.close_section();

  jf.close_section();
  jf.flush(ss);
}

void Monitor::_mon_status(ostream& ss)
{
  JSONFormatter jf(true);
  jf.open_object_section("mon_status");
  jf.dump_string("name", name);
  jf.dump_int("rank", rank);
  jf.dump_string("state", get_state_name());
  jf.dump_int("election_epoch", get_epoch());

  jf.open_array_section("quorum");
  for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
    jf.dump_int("mon", *p);
  jf.close_section();

  jf.open_array_section("outside_quorum");
  for (set<string>::iterator p = outside_quorum.begin(); p != outside_quorum.end(); ++p)
    jf.dump_string("mon", *p);
  jf.close_section();

  if (is_synchronizing()) {
    jf.dump_stream("sync_leader") << sync_leader->entity;
    jf.dump_stream("sync_provider") << sync_provider->entity;
  }

  jf.open_object_section("monmap");
  monmap->dump(&jf);
  jf.close_section();

  jf.close_section();
  
  jf.flush(ss);
}

void Monitor::get_health(string& status, bufferlist *detailbl)
{
  list<pair<health_status_t,string> > summary;
  list<pair<health_status_t,string> > detail;
  for (vector<PaxosService*>::iterator p = paxos_service.begin();
       p != paxos_service.end();
       p++) {
    PaxosService *s = *p;
    s->get_health(summary, detailbl ? &detail : NULL);
  }

  stringstream ss;
  health_status_t overall = HEALTH_OK;
  if (!summary.empty()) {
    ss << ' ';
    while (!summary.empty()) {
      if (overall > summary.front().first)
	overall = summary.front().first;
      ss << summary.front().second;
      summary.pop_front();
      if (!summary.empty())
	ss << "; ";
    }
  }
  stringstream fss;
  fss << overall;
  status = fss.str() + ss.str();

  while (!detail.empty()) {
    detailbl->append(detail.front().second);
    detailbl->append('\n');
    detail.pop_front();
  }
}

void Monitor::handle_command(MMonCommand *m)
{
  if (m->fsid != monmap->fsid) {
    dout(0) << "handle_command on fsid " << m->fsid << " != " << monmap->fsid << dendl;
    reply_command(m, -EPERM, "wrong fsid", 0);
    return;
  }

  MonSession *session = m->get_session();
  if (!session) {
    string rs = "Access denied";
    reply_command(m, -EACCES, rs, 0);
    return;
  }

  bool access_cmd = _allowed_command(session, m->cmd);
  bool access_r = (session->caps.check_privileges(PAXOS_MONMAP, MON_CAP_R) ||
		   access_cmd);
  bool access_all = (session->caps.get_allow_all() || access_cmd);

  dout(0) << "handle_command " << *m << dendl;
  bufferlist rdata;
  string rs;
  int r = -EINVAL;
  rs = "unrecognized command";
  if (!m->cmd.empty()) {
    if (m->cmd[0] == "mds") {
      mdsmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "osd") {
      osdmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "pg") {
      pgmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "mon") {
      monmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "fsid") {
      stringstream ss;
      ss << monmap->fsid;
      reply_command(m, 0, ss.str(), rdata, 0);
      return;
    }
    if (m->cmd[0] == "log") {
      if (!access_r) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      stringstream ss;
      for (unsigned i=1; i<m->cmd.size(); i++) {
	if (i > 1)
	  ss << ' ';
	ss << m->cmd[i];
      }
      clog.info(ss);
      rs = "ok";
      reply_command(m, 0, rs, rdata, 0);
      return;
    }
    if (m->cmd[0] == "stop_cluster") {
      if (!access_all) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      stop_cluster();
      reply_command(m, 0, "initiating cluster shutdown", 0);
      return;
    }

    if (m->cmd[0] == "injectargs") {
      if (!access_all) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      if (m->cmd.size() == 2) {
	dout(0) << "parsing injected options '" << m->cmd[1] << "'" << dendl;
	ostringstream oss;
	g_conf->injectargs(m->cmd[1], &oss);
	derr << "injectargs:" << dendl;
	derr << oss.str() << dendl;
	rs = "parsed options";
	r = 0;
      } else {
	rs = "must supply options to be parsed in a single string";
	r = -EINVAL;
      }
    } 
    if (m->cmd[0] == "class") {
      reply_command(m, -EINVAL, "class distribution is no longer handled by the monitor", 0);
      return;
    }
    if (m->cmd[0] == "auth") {
      authmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "status") {
      if (!access_r) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      // reply with the status for all the components
      string health;
      get_health(health, NULL);
      stringstream ss;
      ss << "   health " << health << "\n";
      ss << "   monmap " << *monmap << "\n";
      ss << "   osdmap " << osdmon()->osdmap << "\n";
      ss << "    pgmap " << pgmon()->pg_map << "\n";
      ss << "   mdsmap " << mdsmon()->mdsmap << "\n";
      rs = ss.str();
      r = 0;
    }
    if (m->cmd[0] == "sync") {
      if (!access_r) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      if (m->cmd[1] == "status") {
	stringstream ss;
	_sync_status(ss);
	rs = ss.str();
	r = 0;
      } else if (m->cmd[1] == "force") {
	stringstream ss;
	_sync_force(ss);
	rs = ss.str();
	r = 0;
      } else {
	rs = "unknown command";
	r = -EINVAL;
	goto out;
      }
    }
    if (m->cmd[0] == "quorum_status") {
      if (!access_r) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      // make sure our map is readable and up to date
      if (!is_leader() && !is_peon()) {
	dout(10) << " waiting for quorum" << dendl;
	waitfor_quorum.push_back(new C_RetryMessage(this, m));
	return;
      }
      stringstream ss;
      _quorum_status(ss);
      rs = ss.str();
      r = 0;
    }
    if (m->cmd[0] == "mon_status") {
      if (!access_r) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      stringstream ss;
      _mon_status(ss);
      rs = ss.str();
      r = 0;
    }
    if (m->cmd[0] == "health") {
      if (!access_r) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      get_health(rs, (m->cmd.size() > 1) ? &rdata : NULL);
      r = 0;
    }
    if (m->cmd[0] == "heap") {
      if (!access_all) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      if (!ceph_using_tcmalloc())
	rs = "tcmalloc not enabled, can't use heap profiler commands\n";
      else
	ceph_heap_profiler_handle_command(m->cmd, clog);
    }
    if (m->cmd[0] == "quorum") {
      if (!access_all) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      if (m->cmd[1] == "exit") {
        reset();
        start_election();
        elector.stop_participating();
        rs = "stopped responding to quorum, initiated new election";
        r = 0;
      } else if (m->cmd[1] == "enter") {
        elector.start_participating();
        reset();
        start_election();
        rs = "started responding to quorum, initiated new election";
        r = 0;
      } else {
	rs = "unknown quorum subcommand; use exit or enter";
	r = -EINVAL;
      }
    }
  }

 out:
  if (!m->get_source().is_mon())  // don't reply to mon->mon commands
    reply_command(m, r, rs, rdata, 0);
  else
    m->put();
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs, version_t version)
{
  bufferlist rdata;
  reply_command(m, rc, rs, rdata, version);
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs, bufferlist& rdata, version_t version)
{
  MMonCommandAck *reply = new MMonCommandAck(m->cmd, rc, rs, version);
  reply->set_data(rdata);
  send_reply(m, reply);
  m->put();
}


// ------------------------
// request/reply routing
//
// a client/mds/osd will connect to a random monitor.  we need to forward any
// messages requiring state updates to the leader, and then route any replies
// back via the correct monitor and back to them.  (the monitor will not
// initiate any connections.)

void Monitor::forward_request_leader(PaxosServiceMessage *req)
{
  int mon = get_leader();
  MonSession *session = 0;
  if (req->get_connection())
    session = (MonSession *)req->get_connection()->get_priv();
  if (req->session_mon >= 0) {
    dout(10) << "forward_request won't double fwd request " << *req << dendl;
    req->put();
  } else if (session && !session->closed) {
    RoutedRequest *rr = new RoutedRequest;
    rr->tid = ++routed_request_tid;
    rr->client = req->get_source_inst();
    encode_message(req, CEPH_FEATURES_ALL, rr->request_bl);   // for my use only; use all features
    rr->session = (MonSession *)session->get();
    routed_requests[rr->tid] = rr;
    session->routed_request_tids.insert(rr->tid);
    
    dout(10) << "forward_request " << rr->tid << " request " << *req << dendl;

    MForward *forward = new MForward(rr->tid, req, rr->session->caps);
    forward->set_priority(req->get_priority());
    messenger->send_message(forward, monmap->get_inst(mon));
  } else {
    dout(10) << "forward_request no session for request " << *req << dendl;
    req->put();
  }
  if (session)
    session->put();
}

//extract the original message and put it into the regular dispatch function
void Monitor::handle_forward(MForward *m)
{
  dout(10) << "received forwarded message from " << m->client
	   << " via " << m->get_source_inst() << dendl;
  MonSession *session = (MonSession *)m->get_connection()->get_priv();
  assert(session);

  if (!session->caps.check_privileges(PAXOS_MONMAP, MON_CAP_X)) {
    dout(0) << "forward from entity with insufficient caps! " 
	    << session->caps << dendl;
  } else {
    Connection *c = new Connection;
    MonSession *s = new MonSession(m->msg->get_source_inst(), c);
    c->set_priv(s);
    c->set_peer_addr(m->client.addr);
    c->set_peer_type(m->client.name.type());

    s->caps = m->client_caps;
    s->proxy_con = m->get_connection()->get();
    s->proxy_tid = m->tid;

    PaxosServiceMessage *req = m->msg;
    m->msg = NULL;  // so ~MForward doesn't delete it
    req->set_connection(c);
    /* Because this is a special fake connection, we need to break
       the ref loop between Connection and MonSession differently
       than we normally do. Here, the Message refers to the Connection
       which refers to the Session, and nobody else refers to the Connection
       or the Session. And due to the special nature of this message,
       nobody refers to the Connection via the Session. So, clear out that
       half of the ref loop.*/
    s->con->put();
    s->con = NULL;

    dout(10) << " mesg " << req << " from " << m->get_source_addr() << dendl;

    _ms_dispatch(req);
  }
  session->put();
  m->put();
}

void Monitor::try_send_message(Message *m, entity_inst_t to)
{
  dout(10) << "try_send_message " << *m << " to " << to << dendl;

  bufferlist bl;
  encode_message(m, CEPH_FEATURES_ALL, bl);  // fixme: assume peers have all features we do.

  messenger->send_message(m, to);

  for (int i=0; i<(int)monmap->size(); i++) {
    if (i != rank)
      messenger->send_message(new MRoute(bl, to), monmap->get_inst(i));
  }
}

void Monitor::send_reply(PaxosServiceMessage *req, Message *reply)
{
  MonSession *session = (MonSession*)req->get_connection()->get_priv();
  if (!session) {
    dout(2) << "send_reply no session, dropping reply " << *reply
	    << " to " << req << " " << *req << dendl;
    reply->put();
    return;
  }
  if (session->proxy_con) {
    dout(15) << "send_reply routing reply to " << req->get_connection()->get_peer_addr()
	     << " via mon" << req->session_mon
	     << " for request " << *req << dendl;
    messenger->send_message(new MRoute(session->proxy_tid, reply),
			    session->proxy_con);    
  } else {
    messenger->send_message(reply, session->con);
  }
  session->put();
}

void Monitor::handle_route(MRoute *m)
{
  MonSession *session = (MonSession *)m->get_connection()->get_priv();
  //check privileges
  if (session && !session->caps.check_privileges(PAXOS_MONMAP, MON_CAP_X)) {
    dout(0) << "MRoute received from entity without appropriate perms! "
	    << dendl;
    session->put();
    m->put();
    return;
  }
  dout(10) << "handle_route " << *m->msg << " to " << m->dest << dendl;
  
  // look it up
  if (m->session_mon_tid) {
    if (routed_requests.count(m->session_mon_tid)) {
      RoutedRequest *rr = routed_requests[m->session_mon_tid];

      // reset payload, in case encoding is dependent on target features
      m->msg->clear_payload();

      messenger->send_message(m->msg, rr->session->inst);
      m->msg = NULL;
      routed_requests.erase(m->session_mon_tid);
      rr->session->routed_request_tids.insert(rr->tid);
      delete rr;
    } else {
      dout(10) << " don't have routed request tid " << m->session_mon_tid << dendl;
    }
  } else {
    dout(10) << " not a routed request, trying to send anyway" << dendl;
    messenger->lazy_send_message(m->msg, m->dest);
    m->msg = NULL;
  }
  m->put();
  if (session)
    session->put();
}

void Monitor::resend_routed_requests()
{
  dout(10) << "resend_routed_requests" << dendl;
  int mon = get_leader();
  for (map<uint64_t, RoutedRequest*>::iterator p = routed_requests.begin();
       p != routed_requests.end();
       p++) {
    RoutedRequest *rr = p->second;

    bufferlist::iterator q = rr->request_bl.begin();
    PaxosServiceMessage *req = (PaxosServiceMessage *)decode_message(cct, q);

    dout(10) << " resend to mon." << mon << " tid " << rr->tid << " " << *req << dendl;
    MForward *forward = new MForward(rr->tid, req, rr->session->caps);
    forward->client = rr->client;
    forward->set_priority(req->get_priority());
    messenger->send_message(forward, monmap->get_inst(mon));
  }  
}

void Monitor::remove_session(MonSession *s)
{
  dout(10) << "remove_session " << s << " " << s->inst << dendl;
  assert(!s->closed);
  for (set<uint64_t>::iterator p = s->routed_request_tids.begin();
       p != s->routed_request_tids.end();
       p++) {
    if (routed_requests.count(*p)) {
      RoutedRequest *rr = routed_requests[*p];
      dout(10) << " dropping routed request " << rr->tid << dendl;
      delete rr;
      routed_requests.erase(*p);
    }
  }
  session_map.remove_session(s);
}


void Monitor::send_command(const entity_inst_t& inst,
			   const vector<string>& com, version_t version)
{
  dout(10) << "send_command " << inst << "" << com << dendl;
  MMonCommand *c = new MMonCommand(monmap->fsid, version);
  c->cmd = com;
  try_send_message(c, inst);
}


void Monitor::stop_cluster()
{
  dout(0) << "stop_cluster -- initiating shutdown" << dendl;
  mdsmon()->do_stop();
}


bool Monitor::_ms_dispatch(Message *m)
{
  bool ret = true;

  if (state == STATE_SHUTDOWN) {
    m->put();
    return true;
  }

  Connection *connection = m->get_connection();
  MonSession *s = NULL;
  bool reuse_caps = false;
  MonCaps caps;
  EntityName entity_name;
  bool src_is_mon;

  src_is_mon = !connection || (connection->get_peer_type() & CEPH_ENTITY_TYPE_MON);

  if (connection) {
    dout(20) << "have connection" << dendl;
    s = (MonSession *)connection->get_priv();
    if (s && s->closed) {
      caps = s->caps;
      reuse_caps = true;
      s->put();
      s = NULL;
    }
    if (!s) {
      if (!exited_quorum.is_zero()
          && !src_is_mon) {
        /**
         * Wait list the new session until we're in the quorum, assuming it's
         * sufficiently new.
         * tick() will periodically send them back through so we can send
         * the client elsewhere if we don't think we're getting back in.
         *
         * But we whitelist a few sorts of messages:
         * 1) Monitors can talk to us at any time, of course.
         * 2) auth messages. It's unlikely to go through much faster, but
         * it's possible we've just lost our quorum status and we want to take...
         * 3) command messages. We want to accept these under all possible
         * circumstances.
         */
        utime_t too_old = ceph_clock_now(g_ceph_context);
        too_old -= g_ceph_context->_conf->mon_lease;
        if (m->get_recv_stamp() > too_old
            && connection->is_connected()) {
          dout(5) << "waitlisting message " << *m
                  << " until we get in quorum" << dendl;
          maybe_wait_for_quorum.push_back(new C_RetryMessage(this, m));
        } else {
          dout(1) << "discarding message " << *m
                  << " and sending client elsewhere; we are not in quorum"
                  << dendl;
          messenger->mark_down(connection);
          m->put();
        }
        return true;
      }
      dout(10) << "do not have session, making new one" << dendl;
      s = session_map.new_session(m->get_source_inst(), m->get_connection());
      m->get_connection()->set_priv(s->get());
      dout(10) << "ms_dispatch new session " << s << " for " << s->inst << dendl;

      if (m->get_connection()->get_peer_type() != CEPH_ENTITY_TYPE_MON) {
	dout(10) << "setting timeout on session" << dendl;
	// set an initial timeout here, so we will trim this session even if they don't
	// do anything.
	s->until = ceph_clock_now(g_ceph_context);
	s->until += g_conf->mon_subscribe_interval;
      } else {
	//give it monitor caps; the peer type has been authenticated
	reuse_caps = false;
	dout(5) << "setting monitor caps on this connection" << dendl;
	if (!s->caps.allow_all) //but no need to repeatedly copy
	  s->caps = *mon_caps;
      }
      if (reuse_caps)
        s->caps = caps;
    } else {
      dout(20) << "ms_dispatch existing session " << s << " for " << s->inst << dendl;
    }
    if (s->auth_handler) {
      entity_name = s->auth_handler->get_entity_name();
    }
  }

  if (s)
    dout(20) << " caps " << s->caps.get_str() << dendl;

  {
    switch (m->get_type()) {
      
    case MSG_ROUTE:
      handle_route((MRoute*)m);
      break;

      // misc
    case CEPH_MSG_MON_GET_MAP:
      handle_mon_get_map((MMonGetMap*)m);
      break;

    case CEPH_MSG_MON_GET_VERSION:
      handle_get_version((MMonGetVersion*)m);
      break;

    case MSG_MON_COMMAND:
      handle_command((MMonCommand*)m);
      break;

    case CEPH_MSG_MON_SUBSCRIBE:
      /* FIXME: check what's being subscribed, filter accordingly */
      handle_subscribe((MMonSubscribe*)m);
      break;

    case MSG_MON_PROBE:
      handle_probe((MMonProbe*)m);
      break;

    // Sync (i.e., the new slurp, but on steroids)
    case MSG_MON_SYNC:
      handle_sync((MMonSync*)m);
      break;

      // OSDs
    case MSG_OSD_FAILURE:
    case MSG_OSD_BOOT:
    case MSG_OSD_ALIVE:
    case MSG_OSD_PGTEMP:
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

    case MSG_REMOVE_SNAPS:
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // MDSs
    case MSG_MDS_BEACON:
    case MSG_MDS_OFFLOAD_TARGETS:
      paxos_service[PAXOS_MDSMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // auth
    case MSG_MON_GLOBAL_ID:
    case CEPH_MSG_AUTH:
      /* no need to check caps here */
      paxos_service[PAXOS_AUTH]->dispatch((PaxosServiceMessage*)m);
      break;

      // pg
    case CEPH_MSG_STATFS:
    case MSG_PGSTATS:
    case MSG_GETPOOLSTATS:
      paxos_service[PAXOS_PGMAP]->dispatch((PaxosServiceMessage*)m);
      break;

    case CEPH_MSG_POOLOP:
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // log
    case MSG_LOG:
      paxos_service[PAXOS_LOG]->dispatch((PaxosServiceMessage*)m);
      break;

      // monmap
    case MSG_MON_JOIN:
      paxos_service[PAXOS_MONMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // paxos
    case MSG_MON_PAXOS:
      {
	if (!src_is_mon && 
	    !s->caps.check_privileges(PAXOS_MONMAP, MON_CAP_X)) {
	  //can't send these!
	  m->put();
	  break;
	}

	MMonPaxos *pm = (MMonPaxos*)m;

	// sanitize
	if (pm->epoch > get_epoch()) {
	  bootstrap();
	  pm->put();
	  break;
	}
	if (pm->epoch != get_epoch()) {
	  pm->put();
	  break;
	}

	paxos->dispatch((PaxosServiceMessage*)m);

	// make sure service finds out about any state changes
	if (paxos->is_active()) {
	  vector<PaxosService*>::iterator service_it = paxos_service.begin();
	  for ( ; service_it != paxos_service.end(); ++service_it)
	    (*service_it)->update_from_paxos();
	}
      }
      break;

      // elector messages
    case MSG_MON_ELECTION:
      //check privileges here for simplicity
      if (s &&
	  !s->caps.check_privileges(PAXOS_MONMAP, MON_CAP_X)) {
	dout(0) << "MMonElection received from entity without enough caps!"
		<< s->caps << dendl;
      }
      if (!is_probing() && !is_synchronizing()) {
	elector.dispatch(m);
      } else {
	m->put();
      }
      break;

    case MSG_FORWARD:
      handle_forward((MForward *)m);
      break;

    default:
      ret = false;
    }
  }
  if (s) {
    s->put();
  }

  return ret;
}

void Monitor::handle_subscribe(MMonSubscribe *m)
{
  dout(10) << "handle_subscribe " << *m << dendl;
  
  bool reply = false;

  MonSession *s = (MonSession *)m->get_connection()->get_priv();
  if (!s) {
    dout(10) << " no session, dropping" << dendl;
    m->put();
    return;
  }

  s->until = ceph_clock_now(g_ceph_context);
  s->until += g_conf->mon_subscribe_interval;
  for (map<string,ceph_mon_subscribe_item>::iterator p = m->what.begin();
       p != m->what.end();
       p++) {
    // if there are any non-onetime subscriptions, we need to reply to start the resubscribe timer
    if ((p->second.flags & CEPH_SUBSCRIBE_ONETIME) == 0)
      reply = true;

    session_map.add_update_sub(s, p->first, p->second.start, 
			       p->second.flags & CEPH_SUBSCRIBE_ONETIME,
			       m->get_connection()->has_feature(CEPH_FEATURE_INCSUBOSDMAP));

    if (p->first == "mdsmap") {
      if ((int)s->caps.check_privileges(PAXOS_MDSMAP, MON_CAP_R)) {
        mdsmon()->check_sub(s->sub_map["mdsmap"]);
      }
    } else if (p->first == "osdmap") {
      if ((int)s->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_R)) {
        osdmon()->check_sub(s->sub_map["osdmap"]);
      }
    } else if (p->first == "monmap") {
      check_sub(s->sub_map["monmap"]);
    } else if ((p->first == "log-error") || (p->first == "log-warn")
	|| (p->first == "log-sec") || (p->first == "log-info") 
	|| (p->first == "log-debug")) {
      logmon()->check_sub(s->sub_map[p->first]);
    }
  }

  // ???

  if (reply)
    messenger->send_message(new MMonSubscribeAck(monmap->get_fsid(), (int)g_conf->mon_subscribe_interval),
			    m->get_source_inst());

  s->put();
  m->put();
}

void Monitor::handle_get_version(MMonGetVersion *m)
{
  dout(10) << "handle_get_version " << *m << dendl;

  MonSession *s = (MonSession *)m->get_connection()->get_priv();
  if (!s) {
    dout(10) << " no session, dropping" << dendl;
    m->put();
    return;
  }

  MMonGetVersionReply *reply = new MMonGetVersionReply();
  reply->handle = m->handle;
  if (m->what == "mdsmap") {
    reply->version = mdsmon()->mdsmap.get_epoch();
    reply->oldest_version = mdsmon()->get_first_committed();
  } else if (m->what == "osdmap") {
    reply->version = osdmon()->osdmap.get_epoch();
    reply->oldest_version = osdmon()->get_first_committed();
  } else if (m->what == "monmap") {
    reply->version = monmap->get_epoch();
    reply->oldest_version = monmon()->get_first_committed();
  } else {
    derr << "invalid map type " << m->what << dendl;
  }

  messenger->send_message(reply, m->get_source_inst());

  s->put();
  m->put();
}

bool Monitor::ms_handle_reset(Connection *con)
{
  dout(10) << "ms_handle_reset " << con << " " << con->get_peer_addr() << dendl;

  if (state == STATE_SHUTDOWN)
    return false;

  // ignore lossless monitor sessions
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
    return false;

  MonSession *s = (MonSession *)con->get_priv();
  if (!s)
    return false;

  Mutex::Locker l(lock);

  dout(10) << "reset/close on session " << s->inst << dendl;
  if (!s->closed)
    remove_session(s);
  s->put();
    
  // remove from connection, too.
  con->set_priv(NULL);
  return true;
}

void Monitor::check_subs()
{
  string type = "monmap";
  if (session_map.subs.count(type) == 0)
    return;
  xlist<Subscription*>::iterator p = session_map.subs[type]->begin();
  while (!p.end()) {
    Subscription *sub = *p;
    ++p;
    check_sub(sub);
  }
}

void Monitor::check_sub(Subscription *sub)
{
  dout(10) << "check_sub monmap next " << sub->next << " have " << monmap->get_epoch() << dendl;
  if (sub->next <= monmap->get_epoch()) {
    send_latest_monmap(sub->session->con);
    if (sub->onetime)
      session_map.remove_sub(sub);
    else
      sub->next = monmap->get_epoch() + 1;
  }
}


// -----

void Monitor::send_latest_monmap(Connection *con)
{
  bufferlist bl;
  monmap->encode(bl, con->get_features());
  messenger->send_message(new MMonMap(bl), con);
}

void Monitor::handle_mon_get_map(MMonGetMap *m)
{
  dout(10) << "handle_mon_get_map" << dendl;
  send_latest_monmap(m->get_connection());
  m->put();
}







/************ TICK ***************/

class C_Mon_Tick : public Context {
  Monitor *mon;
public:
  C_Mon_Tick(Monitor *m) : mon(m) {}
  void finish(int r) {
    mon->tick();
  }
};

void Monitor::new_tick()
{
  C_Mon_Tick *ctx = new C_Mon_Tick(this);
  timer.add_event_after(g_conf->mon_tick_interval, ctx);
}

void Monitor::tick()
{
  // ok go.
  dout(11) << "tick" << dendl;
  
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->tick();
  
  // trim sessions
  utime_t now = ceph_clock_now(g_ceph_context);
  xlist<MonSession*>::iterator p = session_map.sessions.begin();
  while (!p.end()) {
    MonSession *s = *p;
    ++p;
    
    // don't trim monitors
    if (s->inst.name.is_mon())
      continue; 

    if (!s->until.is_zero() && s->until < now) {
      dout(10) << " trimming session " << s->inst
	       << " (until " << s->until << " < now " << now << ")" << dendl;
      messenger->mark_down(s->inst.addr);
      remove_session(s);
    } else if (!exited_quorum.is_zero()) {
      if (now > (exited_quorum + 2 * g_conf->mon_lease)) {
        // boot the client Session because we've taken too long getting back in
        dout(10) << " trimming session " << s->inst
            << " because we've been out of quorum too long" << dendl;
        messenger->mark_down(s->inst.addr);
        remove_session(s);
      }
    }
  }

  if (!maybe_wait_for_quorum.empty()) {
    finish_contexts(g_ceph_context, maybe_wait_for_quorum);
  }

  new_tick();
}

/*
 * this is the closest thing to a traditional 'mkfs' for ceph.
 * initialize the monitor state machines to their initial values.
 */
int Monitor::mkfs(bufferlist& osdmapbl)
{
  MonitorDBStore::Transaction t;

  bufferlist magicbl;
  magicbl.append(CEPH_MON_ONDISK_MAGIC);
  magicbl.append("\n");
  t.put(MONITOR_NAME, "magic", magicbl);

  bufferlist features;
  CompatSet mon_features = get_ceph_mon_feature_compat_set();
  mon_features.encode(features);
  t.put(MONITOR_NAME, COMPAT_SET_LOC, features);

  // save monmap, osdmap, keyring.
  bufferlist monmapbl;
  monmap->encode(monmapbl, CEPH_FEATURES_ALL);
  monmap->set_epoch(0);     // must be 0 to avoid confusing first MonmapMonitor::update_from_paxos()
  t.put("mkfs", "monmap", monmapbl);

  if (osdmapbl.length()) {
    // make sure it's a valid osdmap
    try {
      OSDMap om;
      om.decode(osdmapbl);
    }
    catch (buffer::error& e) {
      derr << "error decoding provided osdmap: " << e.what() << dendl;
      return -EINVAL;
    }
    t.put("mkfs", "osdmap", osdmapbl);
  }

  KeyRing keyring;
  int r = keyring.load(g_ceph_context, g_conf->keyring);
  if (r < 0) {
    derr << "unable to load initial keyring " << g_conf->keyring << dendl;
    return r;
  }

  // put mon. key in external keyring; seed with everything else.
  extract_save_mon_key(keyring);

  bufferlist keyringbl;
  keyring.encode_plaintext(keyringbl);
  t.put("mkfs", "keyring", keyringbl);
  store->apply_transaction(t);

  return 0;
}

int Monitor::write_default_keyring(bufferlist& bl)
{
  ostringstream os;
  os << g_conf->mon_data << "/keyring";

  int err = 0;
  int fd = ::open(os.str().c_str(), O_WRONLY|O_CREAT, 0644);
  if (fd < 0) {
    err = -errno;
    dout(0) << __func__ << " failed to open " << os.str() 
	    << ": " << cpp_strerror(err) << dendl;
    return err;
  }

  err = bl.write_fd(fd);
  if (!err)
    ::fsync(fd);
  ::close(fd);

  return err;
}

void Monitor::extract_save_mon_key(KeyRing& keyring)
{
  EntityName mon_name;
  mon_name.set_type(CEPH_ENTITY_TYPE_MON);
  EntityAuth mon_key;
  if (keyring.get_auth(mon_name, mon_key)) {
    dout(10) << "extract_save_mon_key moving mon. key to separate keyring" << dendl;
    KeyRing pkey;
    pkey.add(mon_name, mon_key);
    bufferlist bl;
    pkey.encode_plaintext(bl);
    write_default_keyring(bl);
    keyring.remove(mon_name);
  }
}

bool Monitor::ms_get_authorizer(int service_id, AuthAuthorizer **authorizer, bool force_new)
{
  dout(10) << "ms_get_authorizer for " << ceph_entity_type_name(service_id) << dendl;

  if (state == STATE_SHUTDOWN)
    return false;

  // we only connect to other monitors; every else connects to us.
  if (service_id != CEPH_ENTITY_TYPE_MON)
    return false;

  if (!auth_supported.is_supported_auth(CEPH_AUTH_CEPHX))
    return false;

  CephXServiceTicketInfo auth_ticket_info;
  CephXSessionAuthInfo info;
  int ret;
  EntityName name;
  name.set_type(CEPH_ENTITY_TYPE_MON);

  auth_ticket_info.ticket.name = name;
  auth_ticket_info.ticket.global_id = 0;

  CryptoKey secret;
  if (!keyring.get_secret(name, secret) &&
      !key_server.get_secret(name, secret)) {
    dout(0) << " couldn't get secret for mon service from keyring or keyserver" << dendl;
    stringstream ss;
    key_server.list_secrets(ss);
    dout(0) << ss.str() << dendl;
    return false;
  }

  /* mon to mon authentication uses the private monitor shared key and not the
     rotating key */
  ret = key_server.build_session_auth_info(service_id, auth_ticket_info, info, secret, (uint64_t)-1);
  if (ret < 0) {
    dout(0) << "ms_get_authorizer failed to build session auth_info for use with mon ret " << ret << dendl;
    return false;
  }

  CephXTicketBlob blob;
  if (!cephx_build_service_ticket_blob(cct, info, blob)) {
    dout(0) << "ms_get_authorizer failed to build service ticket use with mon" << dendl;
    return false;
  }
  bufferlist ticket_data;
  ::encode(blob, ticket_data);

  bufferlist::iterator iter = ticket_data.begin();
  CephXTicketHandler handler(g_ceph_context, service_id);
  ::decode(handler.ticket, iter);

  handler.session_key = info.session_key;

  *authorizer = handler.build_authorizer(0);
  
  return true;
}

bool Monitor::ms_verify_authorizer(Connection *con, int peer_type,
				   int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
				   bool& isvalid)
{
  dout(10) << "ms_verify_authorizer " << con->get_peer_addr()
	   << " " << ceph_entity_type_name(peer_type)
	   << " protocol " << protocol << dendl;

  if (state == STATE_SHUTDOWN)
    return false;

  if (peer_type == CEPH_ENTITY_TYPE_MON &&
      auth_supported.is_supported_auth(CEPH_AUTH_CEPHX)) {
    // monitor, and cephx is enabled
    isvalid = false;
    if (protocol == CEPH_AUTH_CEPHX) {
      bufferlist::iterator iter = authorizer_data.begin();
      CephXServiceTicketInfo auth_ticket_info;
      
      if (authorizer_data.length()) {
	int ret = cephx_verify_authorizer(g_ceph_context, &keyring, iter,
					  auth_ticket_info, authorizer_reply);
	if (ret >= 0)
	  isvalid = true;
	else
	  dout(0) << "ms_verify_authorizer bad authorizer from mon " << con->get_peer_addr() << dendl;
      }
    } else {
      dout(0) << "ms_verify_authorizer cephx enabled, but no authorizer (required for mon)" << dendl;
    }
  } else {
    // who cares.
    isvalid = true;
  }
  return true;
};
