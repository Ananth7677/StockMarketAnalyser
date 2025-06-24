from enum import Enum

class FilingType(str, Enum):
    ANNUAL_REPORT = "10-K"
    QUARTERLY_REPORT = "10-Q"
    
    def __str__(self):
        return self.name.replace("_", " ").title()