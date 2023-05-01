#This is Task 4 part - D from MapReduce Programming Assignment 
# Member 1: Nikhil Dhiman (nikhildhiman3644@gmail.com)
# Member 2: Rohan kulkarni (rohan.kulkarni951@gmail.com)
# Member 3: Mahesh Kumar Patra (maheshkumarpatra5@gmail.com)

#To run this script: 
#python mrtask_a.py <input-file-path> <output-file-path>

from mrjob.job import MRJob
from datetime import datetime

class AverageTripDuration(MRJob):

    def mapper(self, _, line):
        # Ignore header line form csv file
        if line.startswith("VendorID"):
            return  

        # split the line into fields
        fields = line.strip().split(",")
        
        # extract the pickup location ID and trip duration fields
        pickup_location_id = fields[7]
        pickup_time = fields[1]
        dropoff_time = fields[2]
        
        # convert the pickup and dropoff times to datetime objects
        pickup_datetime = datetime.strptime(pickup_time, "%Y-%m-%d %H:%M:%S")
        dropoff_datetime = datetime.strptime(dropoff_time, "%Y-%m-%d %H:%M:%S")
        
        # compute the trip duration in minutes
        trip_duration = (dropoff_datetime - pickup_datetime).total_seconds() / 60
        
        # emit the pickup location ID and trip duration as a tuple
        yield pickup_location_id, trip_duration

    def reducer(self, pickup_location_id, trip_durations):
        # convert the generator object to a list
        trip_durations_list = list(trip_durations)
        
        # compute the total trip duration and number of trips for this pickup location
        total_duration = sum(trip_durations_list)
        num_trips = len(trip_durations_list)
        
        # compute the average trip duration for this pickup location
        avg_duration = total_duration / num_trips
        
        # emit the pickup location ID and average trip duration
        yield pickup_location_id, avg_duration

if __name__ == '__main__':
    AverageTripDuration.run()
