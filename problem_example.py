def check_pair(words):
    assert len(words) == 6, 'pairs must be 6 words'

    A, B, C, X, Y, Z = words

    a = A[0] == X[0]
    if not a:
        return False
    b = A[2] == Y[0]
    if not b:
        return False
    c = A[4] == Z[0]
    if not c:
        return False
    d = B[0] == X[2]
    if not d:
        return False
    e = B[2] == Y[2]
    if not e:
        return False
    f = B[4] == Z[2]
    if not f:
        return False
    g = C[0] == X[4]
    if not g:
        return False
    h = C[2] == Y[4]
    if not h:
        return False
    i = C[4] == Z[4]
    if not i:
        return False

    return True


'''
G O N A D
R   I   I
E I G H T
A   H   T
T A T T Y
'''
pair = [
    'gonad',
    'eight',
    'tatty',
    'great',
    'night',
    'ditty'
]
print(check_pair(pair))
