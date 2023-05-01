#This is Task 4 part - b from MapReduce Programming Assignment 
# Member 1: Nikhil Dhiman (nikhildhiman3644@gmail.com)
# Member 2: Rohan kulkarni (rohan.kulkarni951@gmail.com)
# Member 3: Mahesh Kumar Patra (maheshkumarpatra5@gmail.com)

#To run this script: 
#python mrtask_a.py <input-file-path> <output-file-path>
from mrjob.job import MRJob
from mrjob.step import MRStep

class MostRevenueLocation(MRJob):

    max_pickupId = 0
    max_revenue = 0

    #Mapper fucntion
    def mapper(self, _, line):
        if line.startswith("VendorID"):
            return
        # split the line into fields
        fields = line.strip().split(",")
        
        # extract the pickup location ID and total amount fields
        pickup_location_id = fields[7]
        total_amount = float(fields[16])
        
        # emit the pickup location ID and total amount as a tuple
        yield pickup_location_id, total_amount

    #Reducer fucntion
    def reducer(self, pickup_location_id, total_amounts):
        # compute the total revenue for this pickup location
        total_revenue = sum(total_amounts)
        
        # emit the pickup location ID and total revenue
        yield "Loc",(total_revenue, pickup_location_id)
    
    #Final Reducer
    def reducer_final(self, total_revenue, values):
        # find the pickup location with the maximum revenue
        for (total_revenue, pickup_location_id) in values:
             if total_revenue > self.max_revenue:
                self.max_pickupId = pickup_location_id
                self.max_revenue = total_revenue
        
        # emit the pickup location with the maximum revenue
        yield "Pickup location id with the maximum revenue:", self.max_pickupId 

    #Steps 
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer_final)
        ]

if __name__ == '__main__':
    MostRevenueLocation.run()
