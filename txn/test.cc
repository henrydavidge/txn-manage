#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>

#include <deque>

using std::deque

int main() {
    deque<int> t;
    t.push_back(1);
    t.push_back(2);
    cout << t.front() << endl;
    return 0;
}
