// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)

#include "txn/txn_processor.h"
#include <stdio.h>

#include <set>

#include "txn/lock_manager.h"

#define n 50
#define m 50
// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 100
#define QUEUE_COUNT 10

TxnProcessor::TxnProcessor(CCMode mode)
: mode_(mode), tp_(THREAD_COUNT, QUEUE_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);

  // Start 'RunScheduler()' running as a new task in its own thread.
  tp_.RunTask(
    new Method<TxnProcessor, void>(this, &TxnProcessor::RunScheduler));
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler();
    case LOCKING:                RunLockingScheduler();
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler();
    case OCC:                    RunOCCScheduler();
    case P_OCC:                  RunOCCParallelScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      int blocked = 0;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it))
          blocked++;
      }

      // Request write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
        if (!lm_->WriteLock(txn, *it))
          blocked++;
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed.
      if (blocked == 0)
        ready_txns_.push_back(txn);
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
    }
      // Release write locks.
    for (set<Key>::iterator it = txn->writeset_.begin();
     it != txn->writeset_.end(); ++it) {
      lm_->Release(txn, *it);
  }

      // Commit/abort txn according to program logic's commit/abort decision.
  if (txn->Status() == COMPLETED_C) {
    ApplyWrites(txn);
    txn->status_ = COMMITTED;
  } else if (txn->Status() == COMPLETED_A) {
    txn->status_ = ABORTED;
  } else {
        // Invalid TxnStatus!
    DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
  }

      // Return result to client.
  txn_results_.Push(txn);
}

    // Start executing all transactions that have newly acquired all their
    // locks.
while (ready_txns_.size()) {
      // Get next ready txn from the queue.
  txn = ready_txns_.front();
  ready_txns_.pop_front();

      // Start txn running in its own thread.
  tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
    this,
    &TxnProcessor::ExecuteTxn,
    txn));
}
}
}

void TxnProcessor::RunOCCScheduler() {
  Txn *txn;
  bool valid;
  while (tp_.Active()) {
    // Start a txn
    if (txn_requests_.Pop(&txn)) {
      txn->occ_start_time_ = GetTime();
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
        this, &TxnProcessor::ExecuteTxn, txn));
    }

    while (completed_txns_.Pop(&txn)) {
      if (txn->status_ == COMPLETED_A) {
        txn->status_ = ABORTED;
        txn_results_.Push(txn);
      } else {
        valid = true;
        for (set<Key>::iterator it = txn->readset_.begin();
          it != txn->readset_.end(); ++it) {
          if (storage_.Timestamp(*it) >= txn->occ_start_time_) {
            valid = false;
            goto JUMPTRADING;
          }
        }

        for (set<Key>::iterator it = txn->writeset_.begin();
          it != txn->writeset_.end(); ++it) {
          if (storage_.Timestamp(*it) >= txn->occ_start_time_) {
            valid = false;
            goto JUMPTRADING;
          }
        }

        if (valid) {
          ApplyWrites(txn);
          txn->status_ = COMMITTED;
          txn_results_.Push(txn);
        } else {
          JUMPTRADING:
          txn->occ_start_time_ = GetTime();
          tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this, &TxnProcessor::ExecuteTxn, txn));
        }
      }
    }
  }
}

void TxnProcessor::RunOCCParallelScheduler() {
  // CPSC 438/538:
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may require modifications to other files, most likely
  // txn_processor.h and possibly others.
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  Txn *txn;
  set<Txn*> active_set;
  while (tp_.Active()) {
    // Start a txn
    if (txn_requests_.Pop(&txn)) {
      txn->occ_start_time_ = GetTime();
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
        this, &TxnProcessor::ExecuteTxn, txn));
    }

    // Attempt to validate txns
    // std::cout << "Here1" << std::endl;
    int i = 0, j = 0;
    while (i++ < n && completed_txns_.Pop(&txn)) {
      if (txn->status_ == COMPLETED_A) {
        txn->status_ = ABORTED;
        txn_results_.Push(txn);
      } else {
        active_set = txn_active_set_.GetSet();
        txn_active_set_.Insert(txn);
        tp_.RunTask(new Method<TxnProcessor, void, Txn*, set<Txn*> >(
          this, &TxnProcessor::ValidateOCCP, txn, active_set));
      }
    }

    while (j++ < m && txn_validated_.Pop(&txn))     {
      txn_active_set_.Erase(txn);
      if (txn->validated) {
        txn->status_ = COMMITTED;
        txn_results_.Push(txn);
      } else {
        txn->occ_start_time_ = GetTime();
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
          this, &TxnProcessor::ExecuteTxn, txn));
      }
    }
  }
}


void TxnProcessor::ValidateOCCP(Txn* txn, set<Txn*> active_set) {
  bool valid = true;
  for (set<Key>::iterator it = txn->readset_.begin();
    it != txn->readset_.end(); ++it)  {
    if (storage_.Timestamp(*it) >= txn->occ_start_time_)    {
      valid = false;
      goto JUMPER;
    }
  }

  for (set<Key>::iterator it = txn->writeset_.begin();
    it != txn->writeset_.end(); ++it)  {
    if (storage_.Timestamp(*it) >= txn->occ_start_time_)    {
      valid = false;
      goto JUMPER;
    }
  }

  for (set<Txn*>::iterator t = active_set.begin();
    t != active_set.end(); ++t)  {
    for (set<Key>::iterator it = txn->readset_.begin();
      it != txn->readset_.end(); ++it)    {
      if (storage_.Timestamp(*it) >= txn->occ_start_time_)      {
        valid = false;
        goto JUMPER;
      }
    }

    for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end(); ++it)    {
      if (storage_.Timestamp(*it) >= txn->occ_start_time_)      {
        valid = false;
        goto JUMPER;
      }
    }
  }

  JUMPER:
  if (valid)  {
    ApplyWrites(txn);
  }
  txn->validated = valid;
  txn_validated_.Push(txn);
}

void TxnProcessor::ExecuteTxn(Txn* txn) {
  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
    it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
  if (storage_.Read(*it, &result))
    txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
    it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_.Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
    it != txn->writes_.end(); ++it) {
    storage_.Write(it->first, it->second);
}
}
