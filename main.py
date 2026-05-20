from booking_analysis import booking_analyser

analyzer = booking_analyser()
analyzer.load_from_directory('data/')
print(f"attractions are:, {analyzer.attraction_count}")
print(f"unique types are: , {analyzer.unique_types}")


