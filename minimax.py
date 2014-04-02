#! python3
#  coding=utf-8

data = (
    (((3,17),(2,12)),((15),(25,0))),
    (((2,5),(3)),((2,14)))
)

def minimax(t, depth):
    if hasattr(t,'__iter__'):
        if depth%2:
            ret = min(minimax(o, depth+1) for o in t)
        else:
            ret = max(minimax(x, depth+1) for x in t)
    else:
        ret = t
    return ret

print(minimax(data, 0))
