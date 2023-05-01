#This is Task 4 part - c from MapReduce Programming Assignment 
# Member 1: Nikhil Dhiman (nikhildhiman3644@gmail.com)
# Member 2: Rohan kulkarni (rohan.kulkarni951@gmail.com)
# Member 3: Mahesh Kumar Patra (maheshkumarpatra5@gmail.com)

#To run this script: 
#python mrtask_a.py <input-file-path> <output-file-path>
from mrjob.job import MRJob
from mrjob.step import MRStep

class PaymentTypeCount(MRJob):

    def mapper(self, _, line):
        # ignore header line in csv file
        if line.startswith("VendorID"):
            return    

        # split the line into fields
        fields = line.strip().split(",")
        
        # extract the payment type field
        payment_type = fields[9]
        
        # emit the payment type as the key and a value of 1
        yield payment_type, 1

    def reducer(self, payment_type, counts):
        # sum the counts to find the total number of occurrences of the payment type
        total_count = sum(counts)
        
        # emit the payment type and total count
        yield payment_type, total_count

    def reducer_final(self, payment_type, counts):
        # sum the counts to find the total number of occurrences of the payment type
        total_count = sum(counts)
        
        # emit the payment type and total count in a tuple
        yield None, (payment_type, total_count)

    def reducer_final_sort(self, _, payment_type_counts):
        # sort the payment types by count in descending order
        sorted_payment_type_counts = sorted(payment_type_counts, key=lambda x: -x[1])
        
        # emit the payment type and total count
        for payment_type, count in sorted_payment_type_counts:
            yield payment_type, count
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer_final),
            MRStep(reducer=self.reducer_final_sort)
        ]

if __name__ == '__main__':
    PaymentTypeCount.run()
