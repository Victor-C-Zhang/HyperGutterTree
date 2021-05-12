//
// Created by victor on 5/11/21.
//

#include <gtest/gtest.h>
#include "../include/flush_worker.h"

TEST(FlushWorkerTestSuite, PriorityQueueOrderingTest) {
  FlushWorker fw {nullptr};
  int num = 5;
  BufferControlBlock* bcb[num];
  for (int i = 0; i < num; ++i) {
    bcb[i] = new BufferControlBlock(i, 0, 1);
  }

  fw.request_flush(bcb[0], 1);
  fw.request_flush(bcb[2], 1);
  fw.request_flush(bcb[1], 1);
  fw.request_flush(bcb[3], 2);
  fw.request_flush(bcb[4], 0);

  int expected_1[] = {4, 0, 2, 1, 3};

  int iter = 0;
  for (auto v : fw.flush_queue) {
    ASSERT_EQ(v, bcb[expected_1[iter]]);
    ++iter;
  }

  fw.request_flush(bcb[4], 1);
  fw.request_flush(bcb[2], 1);

  int expected_2[] = {0, 2, 1, 4, 3};
  iter = 0;
  for (auto v : fw.flush_queue) {
    ASSERT_EQ(v, bcb[expected_2[iter]]);
    ++iter;
  }

  for (int i = 0; i < num; ++i) {
    delete(bcb[i]);
  }
}

TEST(FlushWorkerTestSuite, IFrequestsAreMadeAsyncTHENitWorksAsync) {

}
