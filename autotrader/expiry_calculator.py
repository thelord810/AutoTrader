import pendulum
from pendulum.date import Date
import logging

listOfNseHolidays = set([
    Date(2022, 1, 26),  # Republic Day
    Date(2022, 3, 1),  # Maha Shivaratri
    Date(2022, 3, 18),  # Holi
    Date(2022, 4, 14),  # Dr.Baba Saheb Ambedkar Jayanti
    Date(2022, 4, 15),  # Good friday
    Date(2022, 5, 3),  # Id-ul-Fitr
    Date(2022, 8, 9),  # Moharram
    Date(2022, 8, 15),  # Independence Day
    Date(2022, 8, 31),  # Ganesh Chaturthi
    Date(2022, 10, 5),  # Vijaya Dashami
    Date(2022, 10, 24),  # Diwali-Laxmi Pujan
    Date(2022, 10, 26),  # Diwali-Balipratipada
    Date(2022, 11, 8)   # Guru Nanak Jayanti
])

def getNearestWeeklyExpiryDate():
    expiryDate = None
    if(pendulum.now().date().day_of_week is pendulum.THURSDAY):
        expiryDate = pendulum.now()
    else:
        expiryDate = pendulum.now().next(pendulum.THURSDAY)
    return __considerHolidayList(expiryDate).date()


def getNextWeeklyExpiryDate():
    expiryDate = None
    if(pendulum.now().date().day_of_week is pendulum.THURSDAY):
        expiryDate = pendulum.now().next(pendulum.THURSDAY)
    else:
        expiryDate = pendulum.now().next(pendulum.THURSDAY).next(pendulum.THURSDAY)
    return __considerHolidayList(expiryDate).date()


def getNearestMonthlyExpiryDate():
    expiryDate = pendulum.now().last_of('month', pendulum.THURSDAY)
    if(pendulum.now().date() > expiryDate.date()):
        expiryDate = pendulum.now().add(months=1).last_of('month', pendulum.THURSDAY)
    return __considerHolidayList(expiryDate).date()


def getNextMonthlyExpiryDate():
    expiryDate = pendulum.now().last_of('month', pendulum.THURSDAY)
    if(pendulum.now().date() > expiryDate.date()):
        expiryDate = pendulum.now().add(months=2).last_of('month', pendulum.THURSDAY)
    else:
        expiryDate = pendulum.now().add(months=1).last_of('month', pendulum.THURSDAY)
    return __considerHolidayList(expiryDate).date()


# utility method to be used only by this module
def __considerHolidayList(expiryDate: Date):
    if(expiryDate.date() in listOfNseHolidays):
        return __considerHolidayList(expiryDate.subtract(days=1))
    else:
        return expiryDate


# print('today is '+str(pendulum.now().date()))
# print('nearest weekly exp is '+str(getNearestWeeklyExpiryDate()))
# print('next weekly exp is '+str(getNextWeeklyExpiryDate()))
# print('nearest monthly exp is '+str(getNearestMonthlyExpiryDate()))
# print('next month exp is '+str(getNextMonthlyExpiryDate()))