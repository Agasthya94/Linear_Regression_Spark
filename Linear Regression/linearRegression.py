# @Author: Agasthya Vidyanath Rao Peggerla.
#Linear Regression using spark

from  pyspark import SparkContext
import sys
import numpy as np

if __name__ == "__main__":
  if len(sys.argv) !=2:
    print("stderr-Usage: linreg <datafile>")
    exit(-1)
sc = SparkContext(appName="skodem_LinearRegression")
inFile = sc.textFile(sys.argv[1])

'Calculating the product of X and Y matrices'
def X_Y(inp):
	'Split the line to separate y and x values'
	line_split = inp.split(",")
	x_vect = np.array((line_split[1:len(line_split)]),float)
	y_vect = np.array(line_split[0],float)
	'Append an 1 at the beginning of the array'
	x_vect = np.insert(x_vect,0,1)
	'Reshape into a column vector'
	x_vect = x_vect.reshape(len(line_split),1)
	return("x_yvalue",x_vect.dot(y_vect))

'Calculating the product of X and X transpose matrices'

def X_XTranspose(inp):

	'Split the line to separate y and x values'
	line_split = inp.split(",")
	x_vect = np.array((line_split[1:len(line_split)]), float)
	'Append an 1 at the beginning of the array'
	x_vect = np.insert(x_vect,0,1)
	'Reshape into a column vector'
	x_vect = x_vect.reshape(len(line_split),1)
	'Transpose X'
	x_transpose = x_vect.transpose()
	return("x_xtranspose", x_vect.dot(x_transpose))

'Summation of x and x_transpose'
sigmaxxt = inFile.map(X_XTranspose).reduceByKey(lambda a,b: a+b).map(lambda k : k[1])
x_xt = np.array(sigmaxxt.collect())
'Inverse of X*X_Transpose'
xxtinverse = np.linalg.inv(x_xt)

'Summation of x and y'
sigmaxy = inFile.map(X_Y).reduceByKey(lambda a,b: a+b).map(lambda k : k[1])
x_y = np.array(sigmaxy.collect())

'Calculate BETA'
beta = xxtinverse.dot(x_y)

print("DEBUG: Coefficients are as follows: ")
for coefficient in beta:
	print(coefficient)

sc.stop()


