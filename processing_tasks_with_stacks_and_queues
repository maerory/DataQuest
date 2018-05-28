import pandas
from datetime import datetime
applications = pandas.read_csv("applications.csv")

#Function that checks the age by using birthday and checking residency by the address
def process_application(row):
    birth_date = datetime.strptime(application['birthdate'], "%Y-%m-%d %H:%M:%S")
    delta = (datetime.now() - birth_date).total_seconds()
    delta /= (3600 * 24 * 365.25)
    age_check = delta < 18
    residence_check = 'CA' in application['address']
    return age_check and residence_check

application_status = list(applications.apply(process_application, axis=1)) #Use apply function to use function across all rows
