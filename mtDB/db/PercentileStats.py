class PercentileStats:
    def __init__(self, name="percentile"):
        self.percentile = {}
        self.name = name 
    
    
    def update(self, percentile_key, percentile_value):
        self.percentile[percentile_key] = percentile_value