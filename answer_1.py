import pandas as pd
import numpy as np
import os

a = -999
max_n = 0
cols = ["a","b","prod_coef","max_n"]
df = pd.DataFrame(columns=cols)

while a<1000 and a>-1000:
    b=-1000
    while -1000<=b<=1000:
        n = 0
        non_prime=0
        while n < 200:
            if non_prime == 0:
                x = n**2 + a*n + b 
                #print("computing for value of x for n = " + str(n))
                if abs(x) > 1:
                    for i in range(2,abs(x)):
                        if (abs(x) % i) == 0:
                            non_prime +=1
                            max_n = n
            n +=1
        print("for a = "+str(a)+" and b = "+str(b)+", it became a composite number before n = "+str(max_n))
        print("product coefficient is "+ str(a*b))
        if max_n >= 30:
            df = df.append(pd.DataFrame([[a, b, a*b, max_n]], columns=cols), ignore_index=True)
        b += 1
    a += 1

df = df.sort_values(by="max_n", ascending=False).reset_index(drop=True)

if os.path.isfile('../results_part1.csv'):
    os.remove('../results_part1.csv')

print(df[0:5])
df.to_csv('../results_part1.csv')

