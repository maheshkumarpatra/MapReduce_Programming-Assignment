#This is Task 4 part - E from MapReduce Programming Assignment 
# Member 1: Nikhil Dhiman (nikhildhiman3644@gmail.com)
# Member 2: Rohan kulkarni (rohan.kulkarni951@gmail.com)
# Member 3: Mahesh Kumar Patra (maheshkumarpatra5@gmail.com)

#To run this script: 
#python mrtask_a.py <input-file-path> <output-file-path>

from mrjob.job import MRJob
from mrjob.step import MRStep
from operator import itemgetter

class AverageTipsToRevenueRatio(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_step1, reducer=self.reducer_step1),
            MRStep(mapper=self.mapper_step2, reducer=self.reducer_step2),
            MRStep(reducer=self.reducer_step3)
        ]

    def mapper_step1(self, _, line):
        # Ignore first line from csv
        if line.startswith("VendorID"):
            return  

        # parse the input line
        fields = line.split(',')
        # if fields[0] == "VendorID":
        #     return

        pickup_location_id = fields[7]
        tips = float(fields[13])
        revenue = float(fields[16])

        # emit the pickup location ID and tips and revenue values
        # if pickup_location_id == "140":
        yield pickup_location_id, (tips, revenue)
        

    def reducer_step1(self, pickup_location_id, values):
        # use the sum() function to sum the tips and revenue values for each pickup location ID
        # total_tips = sum([v[0] for v in values])
        # total_revenue = sum([v[1] for v in values])
        total_tips = 0
        total_revenue = 0
        for value in values:
            total_tips = total_tips + value[0]
            total_revenue = total_revenue + value[1]

        yield pickup_location_id, (total_tips, total_revenue)

        # emit the pickup location ID and total tips and total revenue values
       

    def mapper_step2(self, pickup_location_id, values):
        # unpack the total tips and total revenue values
        total_tips, total_revenue = values

        # compute the tips to revenue ratio
        if total_revenue == 0:
             ratio = 0
        else:
             ratio = total_tips / total_revenue
        
        # emit the pickup location ID and ratio
        yield pickup_location_id, total_revenue

    def reducer_step2(self, pickup_location_id, ratios):
        # use the sum() function to sum the ratios for each pickup location ID
        total_ratios = sum(ratios)

        # use the len() function to count the number of ratios for each pickup location ID
        num_ratios = len(list(ratios))

        # compute the average ratio
        if num_ratios == 0:
            avg_ratio = 0
        else:
            avg_ratio = total_ratios / num_ratios

        # emit the pickup location ID and average ratio
        yield None, (pickup_location_id, total_ratios)

    # def mapper_step3(self, pickup_location_id, avg_ratio):
    #     # emit the pickup location ID as the key and the average ratio as the value
    #     yield pickup_location_id, avg_ratio

    def reducer_step3(self, pickup_location_id, avg_ratios):
        # use the sorted() function and the itemgetter() function from the operator module to sort the output by pickup location ID
       # convert the avg_ratios generator object to a list
        sorted_avg_ratio = sorted(avg_ratios, key=lambda x: -x[1])
        for pickup_location_id, avg_count_sort in sorted_avg_ratio:
            yield pickup_location_id, avg_count_sort

        # avg_ratios = list(avg_ratios)
        # # sort the list of ratios by pickup location ID
        # avg_ratios = sorted(avg_ratios, key=lambda x: x[0])
        # # iterate over the sorted list of ratios
        # for pickup_location_id, avg_ratio in avg_ratios:
        # # emit the pickup location ID and average ratio
        #     yield pickup_location_id, avg_ratio

if __name__ == '__main__':
    AverageTipsToRevenueRatio.run()
