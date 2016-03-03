import random
import string


def random_id(length=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(length))
