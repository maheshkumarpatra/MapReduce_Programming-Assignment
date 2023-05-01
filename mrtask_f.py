#This is Task 4 part - F from MapReduce Programming Assignment 
# Member 1: Nikhil Dhiman (nikhildhiman3644@gmail.com)
# Member 2: Rohan kulkarni (rohan.kulkarni951@gmail.com)
# Member 3: Mahesh Kumar Patra (maheshkumarpatra5@gmail.com)

#To run this script: 
#python mrtask_a.py <input-file-path> <output-file-path>

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class AverageTripRevenue(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_step1, reducer=self.reducer_step1),
            MRStep(mapper=self.mapper_step2, reducer=self.reducer_step2),
            MRStep(mapper=self.mapper_step3, reducer=self.reducer_step3)
        ]

    def mapper_step1(self, _, line):
        # Remove header form csv
        if line.startswith("VendorID"):
            return    

        # parse the input line
        fields = line.split(',')
        pickup_datetime = fields[1]
        revenue = float(fields[16])

        # convert the pickup datetime string to a datetime object
        pickup_datetime = datetime.strptime(pickup_datetime, '%Y-%m-%d %H:%M:%S')

        # extract the month, hour, and day of the week from the pickup datetime
        month = pickup_datetime.month
        hour = pickup_datetime.hour
        day_of_week = pickup_datetime.weekday()

        # determine whether the hour is during the day or night
        if 6 <= hour < 18:
            hour_of_day = 'day'
        else:
            hour_of_day = 'night'

        # determine whether the day of the week is a weekday or weekend
        if day_of_week in [5, 6]:
            day_of_week = 'weekend'
        else:
            day_of_week = 'weekday'

        # emit the month, hour of the day, and day of the week as the key, and the revenue as the value
        yield (month, hour_of_day, day_of_week), revenue

    def reducer_step1(self, key, revenues):
        # use the sum() function to sum the revenues for each key
        total_revenue = sum(revenues)

        # use the len() function to count the number of revenues for each key
        num_revenues = len(list(revenues))

        # compute the average revenue
        if num_revenues == 0:
             avg_revenue = 0
        else:
             avg_revenue = total_revenue / num_revenues
       
        # emit the key and average revenue
        yield key, avg_revenue

    def mapper_step2(self, key, avg_revenue):
        # emit the key as the key and the average revenue as the value
        yield key, avg_revenue

    def reducer_step2(self, pickup_location_id, values):
        # use the sum() function to sum the values for each pickup location ID
        total_values = sum(values)

        # use the len() function to count the number of values for each pickup location ID
        num_values = len(list(values))

        #compute the average value
        if num_values == 0:
            avg_value = 0
        else:
            avg_value = total_values / num_values

        # emit the pickup location ID and average value
        yield pickup_location_id, avg_value
    
    def mapper_step3(self, key, avg_revenue):
        # emit the key as the key and the average revenue as the value
        yield key, avg_revenue

    def reducer_step3(self, key, avg_revenues):
        # use the sum() function to sum the average revenues for each key
        total_avg_revenues = sum(avg_revenues)

        # use the len() function to count the number of average revenues for each key
        num_avg_revenues = len(list(avg_revenues))

        # compute the final average revenue
        if num_avg_revenues == 0:
            final_avg_revenue =0
        else:
            final_avg_revenue = total_avg_revenues / num_avg_revenues



        # emit the key and final average revenue
        yield key, final_avg_revenue


if __name__ == '__main__':
    AverageTripRevenue.run()

