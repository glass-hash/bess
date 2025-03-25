// Copyright (c) 2014-2016, The Regents of the University of California.
// Copyright (c) 2016-2017, Nefeli Networks, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
// list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// * Neither the names of the copyright holders nor the names of their
// contributors may be used to endorse or promote products derived from this
// software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#include "rte_vport.h"

#include <fcntl.h>
#include <libgen.h>
#include <sched.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <rte_config.h>
#include <rte_malloc.h>

#include "../message.h"
#include "../utils/format.h"

#define BURST_SIZE 32

static const char *_RTE_VPORT_MSG_POOL = "RTE_VPORT_MSG_POOL";
static const char *_RTE_VPORT_SHARED_RING = "RTE_VPORT_SHARED_RING";

struct rte_ring *shared_ring = NULL;
struct rte_mempool *message_pool = NULL;

void RteVPort::InitDriver() {
}

void RteVPort::DeInit() {
    LOG(INFO) << "Entered RteVPort::DeInit";
    if(shared_ring) {
        rte_ring_free(shared_ring);
    }
    if(message_pool) {
        rte_mempool_free(message_pool);
    }
}

CommandResponse RteVPort::Init(const bess::pb::RteVPortArg &arg) {
  (void) arg;
  LOG(INFO) << "Entered RteVPort::Init";
  const unsigned flags = 0;
  const unsigned ring_size = 1024;
  const unsigned pool_size = 4096;
  const unsigned pool_cache = 32;
  const unsigned priv_data_sz = 0;
  shared_ring = rte_ring_create(_RTE_VPORT_SHARED_RING, ring_size, rte_socket_id(), flags);
  message_pool = rte_mempool_create(_RTE_VPORT_MSG_POOL, pool_size,
                                    64, pool_cache, priv_data_sz,
                                    NULL, NULL, NULL, NULL,
                                    rte_socket_id(), flags);
  CommandResponse err;
  return err;
}

int RteVPort::RecvPackets(queue_t qid, bess::Packet **pkts, int max_cnt) {
  (void) qid;
  (void) max_cnt;
  void *pkt[BURST_SIZE] = {NULL};
  int ret = rte_ring_dequeue_bulk(shared_ring, pkt, BURST_SIZE, NULL);
  if(ret == BURST_SIZE) {
      bool result = current_worker.packet_pool()->AllocBulk(pkts, BURST_SIZE, 60);
      if(!result) {
        LOG(INFO) << "Could not allocate packets";
        return 0;
      }
      for (int i = 0; i < BURST_SIZE; i++) {
          bess::Packet *p = pkts[i];
          if(p) {
              char *p_data = p->data();
              rte_memcpy(p_data, pkt[i], 60);
              pkts[i] = p;
          }
          // return the packet back to the shared mempool
          rte_mempool_put(message_pool, pkt[i]);
      }
      return BURST_SIZE;
  }
  return 0;
}

int RteVPort::SendPackets(queue_t qid, bess::Packet **pkts, int cnt) {
  (void) qid;
  (void) pkts;
  (void) cnt;
  return 0;
}

ADD_DRIVER(RteVPort, "rte_vport", "Virtual port for Linux host")
