#ifndef CEPH_RGW_GC_H
#define CEPH_RGW_GC_H


#include "include/types.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "include/rados/librados.hpp"
#include "rgw_rados.h"

class RGWGC {
  CephContext *cct;
  RGWRados *store;
  int max_objs;
  string *obj_names;

  int tag_index(const string& tag);
public:
  RGWGC() : cct(NULL), store(NULL), max_objs(0), obj_names(NULL) {}

  void add_chain(librados::ObjectWriteOperation& op, cls_rgw_obj_chain& chain, const string& tag);
  int send_chain(cls_rgw_obj_chain& chain, const string& tag);

  void initialize(CephContext *_cct, RGWRados *_store);
  void finalize();

  int list(int *index, string& marker, uint32_t max, std::list<cls_rgw_gc_obj_info>& result, bool *truncated);
  void list_init(int *index) { *index = 0; }
  int process(int index, int process_max_secs);
  int process();
};


#endif
