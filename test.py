
import math
import os
import random
import re
import sys
# Complete the sockMerchant function below.
def sockMerchant(n, ar):
    dictionary = {}
    for x in ar:
        if x in dictionary:
            dictionary[x] +=1
        else:
            dictionary[x] = 0
    pair = 0
    for key, value in dictionary.items():
        pair +=  value % 2
    return pair            
if __name__ == '__main__':
    print(sockMerchant(9, [10 20 20 10 10 30 50 10 20 ]))