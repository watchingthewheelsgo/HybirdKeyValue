//
//  microBench.h
//  devl
//
//  Created by 吴海源 on 2018/11/8.
//  Copyright © 2018 why. All rights reserved.
//

#ifndef microBench_h
#define microBench_h

#include <string>
#include <vector>
#include <random>
//#include <chrono>
#include <memory>
#include "Tool.h"
#include "Random.h"

namespace hybridKV {
    
const static int min_key = 8;
const static int max_key = 128;
const static int common_key = 32;

static const int min_val = 16;
static const int max_val = 1024;
static const int common_val = 256;
#define  FALSE          0       // Boolean false
#define  TRUE           1       // Boolean true

//----- Function prototypes -------------------------------------------------
int      zipf(double alpha, int n);  // Returns a Zipf random variable
double   rand_val(int seed);         // Jain's RNG

// //===== Main program ========================================================
// void main(void)
// {
//   FILE   *fp;                   // File pointer to output file
//   char   file_name[256];        // Output file name string
//   char   temp_string[256];      // Temporary string variable
//   double alpha;                 // Alpha parameter
//   double n;                     // N parameter
//   int    num_values;            // Number of values
//   int    zipf_rv;               // Zipf random variable
//   int    i;                     // Loop counter

//   // Output banner
//   printf("---------------------------------------- genzipf.c ----- \n");
//   printf("-     Program to generate Zipf random variables        - \n");
//   printf("-------------------------------------------------------- \n");

//   // Prompt for output filename and then create/open the file
//   printf("Output file name ===================================> ");
//   scanf("%s", file_name);
//   fp = fopen(file_name, "w");
//   if (fp == NULL)
//   {
//     printf("ERROR in creating output file (%s) \n", file_name);
//     exit(1);
//   }

//   // Prompt for random number seed and then use it
//   printf("Random number seed (greater than 0) ================> ");
//   scanf("%s", temp_string);
//   rand_val((int) atoi(temp_string));

//   // Prompt for alpha value
//   printf("Alpha value ========================================> ");
//   scanf("%s", temp_string);
//   alpha = atof(temp_string);

//   // Prompt for N value
//   printf("N value ============================================> ");
//   scanf("%s", temp_string);
//   n = atoi(temp_string);

//   // Prompt for number of values to generate
//   printf("Number of values to generate =======================> ");
//   scanf("%s", temp_string);
//   num_values = atoi(temp_string);

//   // Output "generating" message
//   printf("-------------------------------------------------------- \n");
//   printf("-  Generating samples to file                          - \n");
//   printf("-------------------------------------------------------- \n");

//   // Generate and output zipf random variables
//   for (i=0; i<num_values; i++)
//   {
//     zipf_rv = zipf(alpha, n);
//     fprintf(fp, "%d \n", zipf_rv);
//   }

// }

//===========================================================================
//=  Function to generate Zipf (power law) distributed random variables     =
//=    - Input: alpha and N                                                 =
//=    - Output: Returns with Zipf distributed random variable              =
//===========================================================================
int zipf(double alpha, int n)
{
  static int first = TRUE;      // Static first time flag
  static double c = 0;          // Normalization constant
  static double *sum_probs;     // Pre-calculated sum of probabilities
  double z;                     // Uniform random number (0 < z < 1)
  int zipf_value;               // Computed exponential value to be returned
  int    i;                     // Loop counter
  int low, high, mid;           // Binary-search bounds

  // Compute normalization constant on first call only
  if (first == TRUE)
  {
    for (i=1; i<=n; i++)
      c = c + (1.0 / pow((double) i, alpha));
    c = 1.0 / c;

    sum_probs = (double*)malloc((n+1)*sizeof(*sum_probs));
    sum_probs[0] = 0;
    for (i=1; i<=n; i++) {
      sum_probs[i] = sum_probs[i-1] + c / pow((double) i, alpha);
    }
    first = FALSE;
  }

  // Pull a uniform random number (0 < z < 1)
  do
  {
    z = rand_val(0);
  }
  while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = n, mid;
  do {
    mid = floor((low+high)/2);
    if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
      zipf_value = mid;
      break;
    } else if (sum_probs[mid] >= z) {
      high = mid-1;
    } else {
      low = mid+1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >=1) && (zipf_value <= n));

  return(zipf_value);
}

// int zipf(double alpha, int n)
// {
//   static int first = TRUE;      // Static first time flag
//   static double c = 0;          // Normalization constant
//   double z;                     // Uniform random number (0 < z < 1)
//   double sum_prob;              // Sum of probabilities
//   double zipf_value;            // Computed exponential value to be returned
//   int    i;                     // Loop counter

//   // Compute normalization constant on first call only
//   if (first == TRUE)
//   {
//     for (i=1; i<=n; i++)
//       c = c + (1.0 / pow((double) i, alpha));
//     c = 1.0 / c;
//     first = FALSE;
//   }

//   // Pull a uniform random number (0 < z < 1)
//   do
//   {
//     z = rand_val(0);
//   }
//   while ((z == 0) || (z == 1));

//   // Map z to the value
//   sum_prob = 0;
//   for (i=1; i<=n; i++)
//   {
//     sum_prob = sum_prob + c / pow((double) i, alpha);
//     if (sum_prob >= z)
//     {
//       zipf_value = i;
//       break;
//     }
//   }

//   // Assert that zipf_value is between 1 and N
//   assert((zipf_value >=1) && (zipf_value <= n));

//   return(zipf_value);
// }

//=========================================================================
//= Multiplicative LCG for generating uniform(0.0, 1.0) random numbers    =
//=   - x_n = 7^5*x_(n-1)mod(2^31 - 1)                                    =
//=   - With x seeded to 1 the 10000th x value should be 1043618065       =
//=   - From R. Jain, "The Art of Computer Systems Performance Analysis," =
//=     John Wiley & Sons, 1991. (Page 443, Figure 26.2)                  =
//=========================================================================
double rand_val(int seed)
{
  const long  a =      16807;  // Multiplier
  const long  m = 2147483647;  // Modulus
  const long  q =     127773;  // m div a
  const long  r =       2836;  // m mod a
  static long x;               // Random int value
  long        x_div_q;         // x divided by q
  long        x_mod_q;         // x modulo q
  long        x_new;           // New x value

  // Set the seed if argument is non-zero and then return zero
  if (seed > 0)
  {
    x = seed;
    return(0.0);
  }

  // RNG using integer arithmetic
  x_div_q = x / q;
  x_mod_q = x % q;
  x_new = (a * x_mod_q) - (r * x_div_q);
  if (x_new > 0)
    x = x_new;
  else
    x = x_new + m;

  // Return a random value between 0.0 and 1.0
  return((double) x / m);
}


// Distribution Type:
//  kvNormal for  uniform
//  kvZipf   for  Zipf
//  kvReal   for  dist model in Paper "Workload Analysis for Large-Scale KV Store"
enum kvDistType {
    kvUniform = 0x0,
    kvZipf = 0x1,
    kvReal = 0x2
};

void generatorIndex() {
    
}

//class keyGenerator {
//public:
//    virtual void Next();
//    virtual
//private:
//
//};


std::string randomString(uint32_t len) {
    std::string res(len, ' ');
    for (int i=0; i<len; ++i)
        res[i] = static_cast<char>(' ' + (random() % 95));
    return res;
}

void generateScope(kvDistType type, std::vector<uint32_t>& minLen) {
    int jump = (max_key - min_key) / minLen.size();
    if (type == kvUniform){
        for (int i=0; i<minLen.size(); ++i) {
            minLen[i] = min_key + jump * i;
        }
    } else if (type == kvZipf) {
            
    } else if (type == kvReal) {
        
    }
}

    
// class uniformKeyGenerator {
// public:
//     uniformKeyGenerator(std::vector<uint32_t>& minLen) : rd(), gen(rd()), dist(min_key, max_key), rnd_(5331) {
//     }

//     std::string nextStr() {
//         int idx = dist(gen);
//         return randomString(&rnd_, idx);
//     }

// private:
//     int size;
//     Random rnd_;
//     std::random_device rd;
//     std::mt19937 gen;
//     std::uniform_int_distribution<> dist;
// };
    
class uniformKeyGenerator {
public:
    uniformKeyGenerator(std::vector<uint32_t>& minLen) : size((int)minLen.size()), rd(), gen(rd()), dist(0, size-1), dist_size(min_key, max_key), rnd_(5331), maxKey(size, 0), prefixStr(size) {
        for (auto i=0; i<size; ++i)
            prefixStr[i] = randomString(minLen[i]);
    }
    int nextLen() {
        return dist_size(gen);
    }
    std::string nextStr() {
//        std::string res;
        int idx = dist(gen);
//        LOG("idx = " << idx << " size" << prefixStr.size());
        ++maxKey[idx];
        return prefixStr[idx] + std::to_string(maxKey[idx]-1);
    }
    std::string randomReadStr() {
        int idx = dist(gen);
        return prefixStr[idx] + std::to_string(rnd_.Uniform(maxKey[idx]));
    }
private:
    int size;
    Random rnd_;
    std::random_device rd;
    std::mt19937 gen;
    std::uniform_int_distribution<> dist;
    std::uniform_int_distribution<> dist_size;
    std::vector<uint32_t> maxKey;
    std::vector<std::string> prefixStr;
};

class gevKeyGenerator {
public:
    gevKeyGenerator() : rnd_(5301), rd(), gen(rd()), dist(min_key, max_key), prefixStr(max_key/8+1, "") {
        prefixStr[0] = std::string("abcd");  
        for (int keyLen = min_key; keyLen < max_key; keyLen+=8)
            prefixStr[keyLen/8] = randomString(keyLen);
    }
    std::string nextStr() {
        int keylen = dist(gen);
        // LOG(keylen);
        assert(keylen >= min_key);
        if (keylen < 8)
            return randomString(keylen);
        if (keylen < 16)
            return prefixStr[0] + randomString(keylen % 8 + 4);

        return prefixStr[keylen/8-1] + randomString(keylen % 8 + 8);

    }
private:
    Random rnd_;
    std::random_device rd;
    std::mt19937 gen;
    std::uniform_int_distribution<> dist;
    std::vector<std::string> prefixStr;
};
    
}
#endif /* microBench_h */

