from pyrsistent import PRecord, field

class ListSurveysResultItem(PRecord):
    id = field(str)
    name = field(str)