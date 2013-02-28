// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  if (!lock_table_[key]) {
    lock_table_[key] = new deque<LockRequest>;
  }
  lock_table_[key]->push_back(LockRequest(EXCLUSIVE, txn));

  if (lock_table_[key]->front().txn_ == txn) {
    return true;
  }  else {
    txn_waits_[txn] += 1;
    return false;
  }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  bool change = 0;
  Txn *t;
  if (lock_table_[key]->front().txn_ == txn)
    change = true;

  for (deque<LockRequest>::iterator it = lock_table_[key]->begin();
    it != lock_table_[key]->end(); )  {
    if ((*it).txn_ == txn)    {
      it = lock_table_[key]->erase(it);
    } else    {
      ++it;
    }
  }

  if (change && lock_table_[key]->size())  {
    t = lock_table_[key]->front().txn_;
    txn_waits_[t] -= 1;
    if (txn_waits_[t] == 0)
      ready_txns_->push_back(t);
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  owners->erase(owners->begin(), owners->end());
  if (!lock_table_[key] || !lock_table_[key]->size())
    return UNLOCKED;
  else if (lock_table_[key]->front().mode_ == EXCLUSIVE)  {
    owners->push_back(lock_table_[key]->front().txn_);
    return EXCLUSIVE;
  }
  return UNLOCKED;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  if (!lock_table_[key])  {
    lock_table_[key] = new deque<LockRequest>;
  }
  lock_table_[key]->push_back(LockRequest(EXCLUSIVE, txn));

  if (lock_table_[key]->front().txn_ == txn) {
    return true;
  } else  {
    txn_waits_[txn] += 1;
    return false;
  }
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  if (!lock_table_[key])  {
    lock_table_[key] = new deque<LockRequest>;
  }
  lock_table_[key]->push_back(LockRequest(SHARED, txn));

  if (lock_table_[key]->front().txn_ == txn)
    return true;
  for (deque<LockRequest>::iterator it = lock_table_[key]->begin();
    it != lock_table_[key]->end(); ++it)  {
    if ((*it).mode_ == EXCLUSIVE)    {
      txn_waits_[txn] += 1;
      return false;
    }
  }

  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  Txn *t;
  bool front = false;
  LockMode m;
  deque<LockRequest>::iterator s = lock_table_[key]->end();
  if (lock_table_[key]->front().txn_ == txn)  {
    front = true;
  }

  for (deque<LockRequest>::iterator it = lock_table_[key]->begin();
    it != lock_table_[key]->end(); )  {
    if ((*it).txn_ == txn)    {
      if ((s == lock_table_[key]->end()))
        m = (*it).mode_;
      it = lock_table_[key]->erase(it);
      if (s == lock_table_[key]->end())
        s = it;
    }    else    {
      ++it;
    }
  }

  if (!lock_table_[key]->empty())  {
    if (front && lock_table_[key]->front().mode_ == EXCLUSIVE)    {
      t = lock_table_[key]->front().txn_;
      txn_waits_[t] -= 1;
      if (txn_waits_[t] == 0)
        ready_txns_->push_back(t);
    }    else if (m == EXCLUSIVE)    {
      for (deque<LockRequest>::iterator it = lock_table_[key]->begin();
        it != lock_table_[key]->end() && (*it).mode_ == SHARED; ++it)      {
        if (std::distance(it, s) <= 0) {
          t = (*it).txn_;
         txn_waits_[t] -= 1;
         if (txn_waits_[t] == 0)
          ready_txns_->push_back(t);
      }
    }
  }
}
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  owners->erase(owners->begin(), owners->end());
  if (!lock_table_[key] || !lock_table_[key]->size())
    return UNLOCKED;
  else if (lock_table_[key]->front().mode_ == EXCLUSIVE)  {
    owners->push_back(lock_table_[key]->front().txn_);
    return EXCLUSIVE;
  }  else  {
    for (deque<LockRequest>::iterator it = lock_table_[key]->begin();
      it != lock_table_[key]->end() && (*it).mode_ == SHARED; ++it)    {
      owners->push_back((*it).txn_);
  }
  return SHARED;
}
return UNLOCKED;
return UNLOCKED;
}

