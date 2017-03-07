#core of the clustering algorithm

from pyspark import SparkContext
import math
import sys

clusterCenters = []
distMeasure = 'euclidean'
THRESHOLD = 0.1
ITER_LIMIT = 1000
OUTPUT_PATH = 'file:/home/training/spark_output'
INPUT_PATH = 'file:/home/training/Downloads/Synthetic_Spark_data'
#INPUT_PATH = 'file:/home/training/Downloads/test'

def main(argv):
	k = int(argv[1])
	global clusterCenters
	global distMeasure
	if (len(argv) >= 3):
		distMeasure = argv[2]

	sc = SparkContext("local", "Simple App")
	rawData = sc.textFile(INPUT_PATH)
	data = rawData.map(toList)
	data.persist()

	sample = data.takeSample(False, k) # Initialze cluster centers with k random points
	for point in sample:
		clusterCenters.append(point[0:2])

	avgDistMoved = float('inf')
	newClusterCenters = clusterCenters
	iterations = 0
	while (avgDistMoved > THRESHOLD and iterations < ITER_LIMIT):
		clusterCenters = newClusterCenters
		dataAssignedToClusters = data.map(assignClosestCluster)
		sumsAndCount = dataAssignedToClusters.combineByKey(outputPointAnd1, combineAggregatedAndNewPoint, combineAggregations)
		newCentersRdd = sumsAndCount.map(calcAverage)

		newClusterCenters = [None] * k
		for (cluster, center) in newCentersRdd.collect():
			newClusterCenters[int(cluster)] = center

		distMoved = sumDistMoved(newClusterCenters, clusterCenters)
		avgDistMoved = distMoved / k
		iterations += 1

	print clusterCenters;
	dataAssignedToClusters.saveAsTextFile(OUTPUT_PATH)

"""
Process the data from devicestatus_etl.  We are expecting:
  * Brackets at the beginning and end
  * Each field to have u'(fieldData)', so we strip those out
"""
def toList(line):
	strip = line.strip("[]")
	split = strip.split(",")
	for i in range(len(split)):
		field = split[i]
		field = field.strip();
		split[i] = field[2:-1]
	split[0] = float(split[0])
	split[1] = float(split[1])

	return split

def assignClosestCluster(point):
	return (closestCluster(point), point)

def closestCluster(point): #find the closest cluster for a particular point
	global clusterCenters
	dist = float('inf')
	dist2 = float('inf')
	closestCluster = 0
	for n in range(len(clusterCenters)):
		if distMeasure == 'euclidean':
			dist2 = euclideanDist(point, clusterCenters[n])
		elif distMeasure == 'greatCircle':
			dist2 = greatCircleDist(point, clusterCenters[n])
		if dist2 < dist:
			dist = dist2
			closestCluster = n
	return closestCluster

def outputPointAnd1(point):
	newPoint = point[0:2]
	newPoint.append(1)
	return newPoint

def combineAggregatedAndNewPoint(aggregated, newPoint):
	return [aggregated[0]+newPoint[0], aggregated[1]+newPoint[1], aggregated[2]+1]

def combineAggregations(input1, input2):
	ans = []
	for (a, b) in zip(input1, input2):
		ans.append(a+b)
	return ans

def calcAverage(sumPointAndCount):
	cluster = sumPointAndCount[0]
	actualSumAndCount = sumPointAndCount[1]
	return (cluster, [actualSumAndCount[0] / actualSumAndCount[2], actualSumAndCount[1] / actualSumAndCount[2]])

"""
Sums the pairwise distances between two arrays of cluster centers
"""
def sumDistMoved(clusterCenters1, clusterCenters2):
	total = 0
	for (a,b) in zip(clusterCenters1, clusterCenters2):
		if distMeasure == 'euclidean':
			total += euclideanDist(a, b)
		elif distMeasure == 'greatCircle':
			total += greatCircleDist(a, b)
	return total

def euclideanDist(point1, point2):
	diffLat = (point2[0] - point1[0])
	diffLong = point2[1] - point1[1]
	return (diffLat*diffLat + diffLong*diffLong)

def greatCircleDist(point1, point2):
	toRadians = math.pi/180
	diffLong = (point2[1] - point1[1])*toRadians
	thingToAcos = math.sin(point1[0]*toRadians)*math.sin(point2[0]*toRadians) + math.cos(point1[0]*toRadians)*math.cos(point2[0]*toRadians)*math.cos(diffLong)
	returnVal = math.acos(thingToAcos)*(180/math.pi)
	return returnVal

if __name__ == '__main__':
	main(sys.argv)
