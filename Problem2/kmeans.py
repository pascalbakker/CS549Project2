import numpy as np
data = np.genfromtxt('points.csv', delimiter=',')
centroids = np.genfromtxt('centroids.txt', delimiter=',')

print(centroids)
print("______________")
for iteration in range(1,7):
    #Classify points
    classification = []
    for point in data:
        distance = [np.linalg.norm(point-c) for c in centroids]
        classification.append(distance.index(min(distance)))

    #Obtain new centroids
    centroids = [np.zeros(2) for i in range(4)]
    new_counts = [0]*4
    for i in range(len(classification)):
        c_i = classification[i]
        centroids[c_i] += data[i]
        new_counts[c_i] += 1 

    for i in range(len(centroids)):
        centroids[i] /= new_counts[i]

    print("Results after iteration", iteration)
    for i in centroids:
        print(i)
    print("___________________________")

'''
[[-100. -100.]
 [-100.  100.]
 [ 100. -100.]
 [ 100.  100.]]
______________
Results after iteration 1
[-51.07285371 -54.80906876]
[-17.3789778   22.26832393]
[ 27.36605141 -21.4832891 ]
[39.06861419 48.02057932]
___________________________
Results after iteration 2
[-54.36085054 -62.53361012]
[-19.55459593   9.76653171]
[ 27.52514849 -13.73156084]
[42.57015786 54.71264246]
___________________________
Results after iteration 3
[-55.46159265 -66.26772966]
[-24.07788447   3.29018613]
[34.35223881 -8.46919844]
[41.05009046 58.25692936]
___________________________
Results after iteration 4
[-56.87029308 -67.84729116]
[-26.60391843  -4.1454449 ]
[37.5153542  -2.21321471]
[39.27690457 59.54087628]
___________________________
Results after iteration 5
[-57.61764389 -69.25733569]
[-29.16137759  -8.6875808 ]
[37.94555131  1.69580331]
[38.01378455 60.83203555]
___________________________
Results after iteration 6
[-58.00997638 -70.81839493]
[-30.61742873 -10.55158112]
[37.41035352  2.25939692]
[38.32473137 61.17741008]
___________________________
'''