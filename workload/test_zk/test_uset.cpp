#include <iostream>
#include <unordered_set>
#include <chrono>

using namespace std;

int main() {

  // uniform initialization
  unordered_set<int> ids;
  using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

	int N  = 1000000000;
	
  for(int i =0; i<N; i++){
	ids.insert(i);
  }
  
 // ids.find();
  // loop across the unordered set
  // display the value
  //cout << "numbers = ";
  //for(auto &num: numbers) {
  //  cout << num << ", ";
  //}

    auto t1 = high_resolution_clock::now();
    ids.find(20000000);
    auto t2 = high_resolution_clock::now();

    /* Getting number of milliseconds as an integer. */
    auto ms_int = duration_cast<milliseconds>(t2 - t1);

    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;

    std::cout << ms_int.count() << "ms\n";
    std::cout << ms_double.count() << "ms\n";

 
    bool ibs[1000000];
   int index = 199;



    t1 = high_resolution_clock::now();
    	 if(ibs[index] == true);
    t2 = high_resolution_clock::now();
	 ms_int = duration_cast<milliseconds>(t2 - t1);

    /* Getting number of milliseconds as a double. */
   ms_double = t2 - t1;

    std::cout << ms_int.count() << "ms\n";
    std::cout << ms_double.count() << "ms\n";


  return 0;
}

//#include <unordered_set>

