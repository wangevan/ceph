// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef CEPH_MMONSYNC_H
#define CEPH_MMONSYNC_H

#include "msg/Message.h"

class MMonSync : public Message
{
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:
  /**
  * Operation types
  */
  enum {
    /**
    * Start synchronization request
    * (mon.X -> Leader)
    */
    OP_START		= 1,
    /**
     * Reply to an OP_START
     * (Leader -> mon.X)
     */
    OP_START_REPLY	= 2,
    /**
     * Let the Leader know we are still synchronizing
     * (mon.X -> Leader)
     */
    OP_HEARTBEAT	= 3,
    /**
     * Reply to a hearbeat
     * (Leader -> mon.X)
     */
    OP_HEARTBEAT_REPLY	= 4,
    /**
     * Let the Leader know we finished synchronizing
     * (mon.X -> Leader)
     */
    OP_FINISH		= 5,
    /**
     * Request a given monitor (mon.Y) to start synchronizing with us, hence
     * sending us chunks.
     * (mon.X -> mon.Y)
     */
    OP_START_CHUNKS	= 6,
    /**
     * Send a chunk to a given monitor (mon.X)
     * (mon.Y -> mon.X)
     */
    OP_CHUNK		= 7,
    /**
     * Acknowledge that we received the last chunk sent
     * (mon.X -> mon.Y)
     */
    OP_CHUNK_REPLY	= 8,
  };

  /**
  * Chunk is the last available
  */
  const static uint8_t FLAG_LAST      = 0x01;
 /**
  * Let the other monitor it should retry again its last operation.
  */
  const static uint8_t FLAG_RETRY     = 0x02;

  /**
  * Obtain a string corresponding to the operation type @p op
  *
  * @param op Operation type
  * @returns A string
  */
  static const char *get_opname(int op) {
    switch (op) {
    case OP_START: return "start"; break;
    case OP_START_REPLY: return "start_reply"; break;
    case OP_HEARTBEAT: return "heartbeat"; break;
    case OP_FINISH: return "finish"; break;
    case OP_START_CHUNKS: return "start_chunks"; break;
    case OP_CHUNK: return "chunk"; break;
    case OP_CHUNK_REPLY: return "chunk_reply"; break;
    default: assert("unknown op type"); return NULL;
    }
  }

  uint32_t op;
  uint8_t flags;
  version_t version;
  bufferlist chunk_bl;
  pair<string,string> first_key;
  pair<string,string> last_key;

  MMonSync(uint32_t op, bufferlist bl, uint8_t flags = 0) 
    : op(op), flags(flags), version(0), chunk_bl(bl)
  { }

  MMonSync(uint32_t op)
    : op(op), flags(0), version(0)
  { }

  /**
  * Obtain this message type's name */
  const char *get_type_name() const { return "mon_sync"; }

  /**
  * Print this message in a pretty format to @p out
  *
  * @param out The output stream to output to
  */
  void print(ostream& out) const {
    out << "mon_sync( " << get_opname(op);

    if (version > 0)
      out << " v " << version;

    if (flags) {
      out << " flags( ";
      if (flags & FLAG_LAST)
	out << "last ";
      if (flags & FLAG_RETRY)
	out << "retry ";
      out << ")";
    }

    if (chunk_bl.length())
      out << " bl " << chunk_bl.length() << " bytes";

    if (!first_key.first.empty() || !first_key.second.empty()) {
      out << " first_key ( " << first_key.first << ","
	  << first_key.second << " )";
    }
    
    if (!last_key.first.empty() || !last_key.second.empty()) {
      out << " last_key ( " << last_key.first << ","
	  << last_key.second << " )";
    }

    out << " )";	
  }

  /**
  * Encode this message into the Message's payload
  */
  void encode_payload(uint64_t features) {
    ::encode(op, payload);
    ::encode(flags, payload);
    ::encode(version, payload);
    ::encode(chunk_bl, payload);
    ::encode(first_key.first, payload);
    ::encode(first_key.second, payload);
    ::encode(last_key.first, payload);
    ::encode(last_key.second, payload);
  }

  /**
  * Decode the message's payload into this message
  */
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(flags, p);
    ::decode(version, p);
    ::decode(chunk_bl, p);
    ::decode(first_key.first, p);
    ::decode(first_key.second, p);
    ::decode(last_key.first, p);
    ::decode(last_key.second, p);
  }
};

#endif /* CEPH_MMONSYNC_H */
