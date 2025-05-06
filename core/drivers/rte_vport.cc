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

#include <rte_flow.h>

struct rte_ether_addr src_mac = {{0xb4, 0x96, 0x91, 0xa4, 0x02, 0xe9}};
struct rte_ether_addr dst_mac = {{0xb4, 0x96, 0x91, 0xa4, 0x04, 0x21}};

#define BURST_SIZE 32
#define NSEC_PER_SEC 1000000000L

struct rte_ring **shared_rings = NULL;
struct rte_mempool **mempools = NULL;

void RteVPort::InitDriver() {}

void RteVPort::DeInit() {
  LOG(INFO) << "Entered RteVPort::DeInit";
  if (shared_rings) {
    for (int i = 0; i < num_cores; i++) {
      rte_ring_free(shared_rings[i]);
    }
    free(shared_rings);
  }

  if (mempools) {
    for (int i = 0; i < num_cores; i++) {
      rte_mempool_free(mempools[i]);
    }
    free(mempools);
  }

  if (test_pkt) {
    free(test_pkt);
  }
}

uint8_t *generate_pkt(uint16_t pkt_size) {
  uint8_t *pkt_buf = (uint8_t *)malloc(pkt_size * sizeof(uint8_t));
  if (pkt_buf == NULL) {
    std::cerr << "Failed to allocate memory from malloc" << std::endl;
    exit(0);
  }
  uint16_t pkt_size_no_crc = pkt_size - RTE_ETHER_CRC_LEN;  // no crc

  // Setup packet headers
  struct rte_ether_hdr *eth_hdr = (struct rte_ether_hdr *)pkt_buf;
  struct rte_ipv4_hdr *ip_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 1);
  struct rte_udp_hdr *udp_hdr = (struct rte_udp_hdr *)(ip_hdr + 1);

  // Ethernet header
  eth_hdr->s_addr = src_mac;
  eth_hdr->d_addr = dst_mac;
  eth_hdr->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

  // IP header
  ip_hdr->version_ihl = RTE_IPV4_VHL_DEF;
  ip_hdr->type_of_service = 0;
  ip_hdr->total_length =
      rte_cpu_to_be_16(pkt_size_no_crc - sizeof(struct rte_ether_hdr));
  ip_hdr->packet_id = 0;
  ip_hdr->fragment_offset = 0;
  ip_hdr->time_to_live = 64;
  ip_hdr->next_proto_id = IPPROTO_UDP;
  ip_hdr->src_addr = rte_cpu_to_be_32(0x0A000001);
  ip_hdr->dst_addr = rte_cpu_to_be_32(0xC0A80000);
  ip_hdr->hdr_checksum = 0;
  ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

  // UDP header
  udp_hdr->src_port = rte_cpu_to_be_16(8080);
  udp_hdr->dst_port = rte_cpu_to_be_16(80);
  udp_hdr->dgram_len =
      rte_cpu_to_be_16(pkt_size_no_crc - (sizeof(struct rte_ether_hdr) +
                                          sizeof(struct rte_ipv4_hdr)));
  udp_hdr->dgram_cksum = 0;

  uint8_t *payload =
      (uint8_t *)(((char *)udp_hdr) + sizeof(struct rte_udp_hdr));
  uint64_t payload_size = pkt_size_no_crc - sizeof(struct rte_ether_hdr) -
                          sizeof(struct rte_ipv4_hdr) -
                          sizeof(struct rte_udp_hdr);
  for (uint64_t i = 0; i < payload_size; ++i) {
    payload[i] = 0xff;
  }
  return pkt_buf;
}

static inline uint64_t get_ns(void) {
  struct timespec ts;
  // Get current time using CLOCK_MONOTONIC
  clock_gettime(CLOCK_MONOTONIC, &ts);
  // Convert to nanoseconds
  uint64_t ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
  return ns;
}

void RteVPort::RatePrecompute() {
  uint64_t factor = NSEC_PER_SEC;
  mult = 1;
  shift = 0;
  if (tbf_rate <= 0)
    return;

  for (;;) {
    mult = factor / tbf_rate;
    if (mult & (1U << 31) || factor & (1ULL << 63))
      break;
    factor <<= 1;
    (shift)++;
  }
}

CommandResponse RteVPort::Init(const bess::pb::RteVPortArg &arg) {
  num_cores = arg.num_cores();
  pkt_size = arg.pkt_size();
  pkt_size_on_wire = pkt_size + 20;
  pkt_size_no_crc = pkt_size - RTE_ETHER_CRC_LEN;  // no crc

  tbf_rate = ((uint64_t)arg.tbf_rate() * 1000000000) / 8;
  uint64_t max_burst_size =
      (uint64_t)((uint64_t)arg.tbf_burst() * 1024 * 1024 * 1024) / 8;
  buffer = (int64_t)(max_burst_size * NSEC_PER_SEC) / tbf_rate;
  last_ckpt = get_ns();
  tokens = buffer;
  tokens_lc = 0;
  now = 0;

  RatePrecompute();
  LOG(INFO) << "Rate is " << tbf_rate << " buffer is " << buffer;

  cur_core_ind = 0;
  // As per DPDK docs, optimum ring size is 2^n and pool size is 2^n - 1
  const unsigned ring_size = 1024;
  const unsigned pool_size = 1023;
  const unsigned priv_data_sz = 0;
  CommandResponse err;
  shared_rings =
      (struct rte_ring **)malloc(num_cores * sizeof(struct rte_ring *));
  if (shared_rings == NULL) {
    LOG(FATAL) << "Memory allocation failed";
    return CommandFailure(ENOMEM, "shared_rings memory allocation failed");
  }

  mempools =
      (struct rte_mempool **)malloc(num_cores * sizeof(struct rte_mempool *));
  if (shared_rings == NULL) {
    LOG(FATAL) << "Memory allocation failed";
    return CommandFailure(ENOMEM, "mempools memory allocation failed");
  }

  for (uint16_t i = 0; i < num_cores; i++) {
    char SHARED_RING_NAME[30];
    sprintf(SHARED_RING_NAME, "RTE_VPORT_SHARED_RING_%u", i);
    LOG(INFO) << "Creating ring and mempool for core " << i;
    shared_rings[i] =
        rte_ring_create(SHARED_RING_NAME, ring_size, rte_socket_id(),
                        RING_F_SP_ENQ | RING_F_SC_DEQ);
    if (shared_rings[i] == NULL) {
      return CommandFailure(ENOMEM, "rte_ring_create failed");
    }

    char MEMPOOL_NAME[30];
    sprintf(MEMPOOL_NAME, "RTE_VPORT_MEMPOOL_%u", i);
    mempools[i] = rte_mempool_create(
        MEMPOOL_NAME, pool_size, pkt_size, 0, priv_data_sz, NULL, NULL, NULL,
        NULL, rte_socket_id(), MEMPOOL_F_SP_PUT | MEMPOOL_F_SC_GET);
    if (mempools[i] == NULL) {
      return CommandFailure(ENOMEM, "rte_mempools failed");
    }
  }

  test_pkt = generate_pkt(pkt_size);
  return err;
}

#ifdef RTE_VPORT_NO_COORD
// In this case, we copy the same packet over and over in BESS and don't
// communicate with the application cores. This will perform the best. Remember
// in this case, you do not need to start any applications.
int RteVPort::RecvPackets(queue_t qid, bess::Packet **pkts, int max_cnt) {
  (void)qid;
  (void)max_cnt;
  bool result = current_worker.packet_pool()->AllocBulk(pkts, BURST_SIZE,
                                                        pkt_size_no_crc);
  if (!result) {
    LOG(INFO) << "Could not allocate packets";
    return 0;
  }
  for (int i = 0; i < BURST_SIZE; i++) {
    bess::Packet *p = pkts[i];
    char *ptr = p->buffer<char *>() + SNBUF_HEADROOM;
    rte_memcpy(ptr, test_pkt, pkt_size_no_crc);
  }
  return BURST_SIZE;
}
#elif RTE_VPORT_NO_COPY
// In this case, we dequeue the packets but end up freeing them. There are cache
// invalidtions happening between the application and the core where BESS runs.
// We do not copy anything here and just send whatever BESS allocates. This will
// perform the second best. This function assumes only one core.
int RteVPort::RecvPackets(queue_t qid, bess::Packet **pkts, int max_cnt) {
  (void)qid;
  (void)max_cnt;
  void *client_pkts[BURST_SIZE] = {NULL};
  int ret =
      rte_ring_dequeue_bulk(shared_rings[0], client_pkts, BURST_SIZE, NULL);
  if (ret == BURST_SIZE) {
    bool result = current_worker.packet_pool()->AllocBulk(pkts, BURST_SIZE,
                                                          pkt_size_no_crc);
    if (!result) {
      LOG(INFO) << "Could not allocate packets";
      return 0;
    }
    rte_mempool_put_bulk(mempools[0], client_pkts, BURST_SIZE);
    return BURST_SIZE;
  }
  return 0;
}
#else
// In this case, we dequeue the packets, and copy them on the packets allocated
// by BESS. This will perform the worst.
int RteVPort::RecvPackets(queue_t qid, bess::Packet **pkts, int max_cnt) {
  (void)qid;
  (void)max_cnt;
  void *client_pkts[BURST_SIZE] = {NULL};
  int total_sent = 0;
  now = get_ns();
  tokens_lc = ((now - last_ckpt) < buffer) ? (now - last_ckpt) : buffer;
  tokens_lc += tokens;
  if (tokens_lc > buffer)
    tokens_lc = buffer;
  int64_t total_size_on_wire = BURST_SIZE * pkt_size_on_wire;
  tokens_lc -= ((total_size_on_wire * mult) >> shift);
  // tokens_lc -= (int64_t)(total_size_on_wire * NSEC_PER_SEC) / tbf_rate;
  if (tokens_lc >= 0) {
    // dequeue the packets
    int ret = rte_ring_sc_dequeue_bulk(shared_rings[cur_core_ind], client_pkts,
                                       BURST_SIZE, NULL);
    if (likely(ret == BURST_SIZE)) {
      bool result = current_worker.packet_pool()->AllocBulk(pkts, BURST_SIZE,
                                                            pkt_size_no_crc);
      if (!result) {
        LOG(INFO) << "Could not allocate packets";
        return 0;
      }
      // Copy the packets from the application on to the allocated packets
      for (uint16_t i = 0; i < BURST_SIZE; i++) {
        bess::Packet *p = pkts[i];
        char *ptr = p->buffer<char *>() + SNBUF_HEADROOM;
        p->set_data_off(SNBUF_HEADROOM);
        p->set_total_len(pkt_size_no_crc);
        p->set_data_len(pkt_size_no_crc);
        rte_memcpy(ptr, client_pkts[i], pkt_size_no_crc);
      }
      total_sent = BURST_SIZE;
      last_ckpt = now;
      tokens = tokens_lc;
      // Return the pointers to the mempool
      rte_mempool_put_bulk(mempools[cur_core_ind], client_pkts, BURST_SIZE);
    }
  }
  cur_core_ind = (cur_core_ind + 1) % num_cores;
  return total_sent;
}
#endif

int RteVPort::SendPackets(queue_t qid, bess::Packet **pkts, int cnt) {
  (void)qid;
  (void)pkts;
  (void)cnt;
  return 0;
}

ADD_DRIVER(RteVPort, "rte_vport", "Virtual port for Linux host")
