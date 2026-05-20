import json
import os

class booking:
    def __init__(self, booking_data):
        self.data = booking_data
    
    def get_type(self):
        return self.data['type']
    
    def get_name(self):
        return self.data['metadata']['name']
    
    def get_location(self):
        return self.data['location']        
        
        
class booking_analyser:
    def load_from_directory(self, directory_path):
        for filename in os.listdir(directory_path):
            if filename.endswith('.json'):
                self.load_from_file(os.path.join(directory_path, filename))

    def load_from_file(self, file_path):
        with open(file_path, 'r') as file:
            data = json.load(file)
            for booking_data in data['bookings']:
                self.update(booking_data)
    
    def __init__(self):
        self.bookings = []
        self.attraction_count = 0
        self.unique_types = set()
        self.booking_attraction_detail = []
    
    def update(self, booking_data):
        b = booking(booking_data)
        self.bookings.append(b)
        if b.get_type() == "attraction":
            self.attraction_count += 1
        self.unique_types.add(b.get_type())
        if b.get_type() == "attraction":
            self.booking_attraction_detail.append({
                "name": b.get_name(),
                "location": b.get_location()})
        