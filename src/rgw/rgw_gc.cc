

#include "rgw_gc.h"
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_client.h"


using namespace librados;

static string gc_oid_prefix = "gc";

void RGWGC::initialize(CephContext *_cct, RGWRados *_store) {
  cct = _cct;
  store = _store;

  max_objs = cct->_conf->rgw_gc_max_objs;
  obj_names = new string[max_objs];

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = gc_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf);
  }
}

void RGWGC::finalize()
{
  for (int i = 0; i < max_objs; i++) {
    delete[] obj_names;
  }
}

int RGWGC::tag_index(const string& tag)
{
  return ceph_str_hash_linux(tag.c_str(), tag.size()) % max_objs;
}

void RGWGC::add_chain(ObjectWriteOperation& op, cls_rgw_obj_chain& chain, const string& tag)
{
  cls_rgw_gc_obj_info info;
  info.chain = chain;
  info.tag = tag;

  cls_rgw_gc_set_entry(op, cct->_conf->rgw_gc_obj_min_wait, info);
}

int RGWGC::send_chain(cls_rgw_obj_chain& chain, const string& tag)
{
  ObjectWriteOperation op;
  add_chain(op, chain, tag);

  int i = tag_index(tag);

  return store->gc_operate(obj_names[i], &op);
}
