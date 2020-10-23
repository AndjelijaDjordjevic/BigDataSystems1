import sys
from pyspark import SparkConf, SparkContext


def main(filepath, latitude_min, latitude_max, longitude_min, longitude_max, time_min, time_max):
    conf = SparkConf().setAppName("BigData1")
    sc = SparkContext(conf=conf)

    sc.setLogLevel("ERROR")
    rdd_data = sc.textFile(filepath)
    rdd_data.cache()

    rdd_data_split = rdd_data.map(lambda line: line.split(","))
    # print(rdd_data_split.take(2))
    # print(rdd_data_split.count())

    rdd_label_formated = rdd_data_split.map(lambda tup: (float(tup[0]), float(tup[1]), float(tup[2]), int(tup[3]),
                                                         float(tup[4]), float(tup[5])))
    # print(rdd_label_formated.take(2))

    # latitude_min = 50.83
    # latitude_max = 50.85
    #
    # longitude_min = -0.14
    # longitude_max = 0

    # time_min = 1498121165520
    # time_max = 1498151168140
    rdd_filtered_time_location = rdd_label_formated.filter(lambda x: latitude_min < float(x[1]) < latitude_max and
                                                                     longitude_min < float(x[2]) < longitude_max and
                                                                     time_min < float(x[0]) < time_max)
    rdd_filtered_time_location.cache()

    print("rdd_filtered_time_location.count()", rdd_filtered_time_location.count())
    rdd_filtered_walking = rdd_filtered_time_location.filter(lambda x: x[3] == 2)
    print("Walking time " + str(rdd_filtered_walking.count()) + " sec")

    min_pressure = rdd_filtered_time_location.map(lambda x: x[4]).fold(0, lambda x, y: min(x, y))
    max_pressure = rdd_filtered_time_location.map(lambda x: x[4]).fold(0, lambda x, y: max(x, y))

    print("Min pressure " + str(min_pressure))
    print("Max pressure " + str(max_pressure))

    min_temperature = rdd_filtered_time_location.map(lambda x: x[5]).fold(0, lambda x, y: min(x, y))
    max_temperature = rdd_filtered_time_location.map(lambda x: x[5]).fold(0, lambda x, y: max(x, y))

    print("Min temperature " + str(min_temperature))
    print("Max temperature " + str(max_temperature))

    avg_pressure = rdd_filtered_time_location.aggregate((0.0, 0.0),
                                                        (lambda acc, value: (acc[0] + value[4], acc[1] + 1)),
                                                        (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])))
    # print(avg_pressure)
    print("Average pressure " + str(avg_pressure[0] / avg_pressure[1]))


if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("\nBad command syntax. The correct parameter syntax is:")
        print("filepath, latitude_min, latitude_max, longitude_min, longitude_max, time_min, time_max\n")
    else:
        good = True
        try:
            filepath = sys.argv[1]
            latitude_min = float(sys.argv[2])
            latitude_max = float(sys.argv[3])
            longitude_min = float(sys.argv[4])
            longitude_max = float(sys.argv[5])
            time_min = float(sys.argv[6])
            time_max = float(sys.argv[7])
        except ValueError:
            print("\nInvalid parameters.\n")
            good = False
        if good:
            main(filepath, latitude_min, latitude_max, longitude_min, longitude_max, time_min, time_max)
