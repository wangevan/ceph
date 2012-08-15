

#include "rgw_gc.h"
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_client.h"

#include <list>


using namespace std;
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

int RGWGC::list(int *index, string& marker, uint32_t max, std::list<cls_rgw_gc_obj_info>& result, bool *truncated)
{
  result.clear();

  for (; *index < cct->_conf->rgw_gc_max_objs && result.size() < max; (*index)++, marker.clear()) {
    std::list<cls_rgw_gc_obj_info> entries;
    int ret = cls_rgw_gc_list(store->gc_pool_ctx, obj_names[*index], marker, max - result.size(), entries, truncated);
    if (ret == -ENOENT)
      continue;
    if (ret < 0)
      return ret;

    std::list<cls_rgw_gc_obj_info>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      result.push_back(*iter);
    }

    if (*index == cct->_conf->rgw_gc_max_objs - 1) {
      /* we cut short here, truncated will hold the correct value */
      return 0;
    }

    if (result.size() == max) {
      /* close approximation, it might be that the next of the objects don't hold
       * anything, in this case truncated should have been false, but we can find
       * that out on the next iteration
       */
      *truncated = true;
      return 0;
    }

  }
  *truncated = false;

  return 0;
}

