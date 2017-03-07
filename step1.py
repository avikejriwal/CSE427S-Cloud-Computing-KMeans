#load the dataset, and filter sample data

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
mydata = sc.textFile("file:/home/training/training_materials/data/devicestatus.txt") #load file into spark

def splitLine(line):
	delimiter = line[19]
	return line.split(delimiter)


splitToRecords = mydata.map(splitLine)
validRecords = splitToRecords.filter(lambda words: len(words) == 14)
hasValidCoordinates = validRecords.filter(lambda words: (words[-1] != "0" or words[-2] != "0"))

separateMakeModel = hasValidCoordinates.map(lambda words: words[0:1] + words[1].split(" ", 1)+ words[2:])

extractedData = separateMakeModel.map(lambda words: [words[-2],words[-1]] + words[0:3])

print extractedData.take(5)

extractedData.saveAsTextFile("/loudacre/devicestatus_etl")
