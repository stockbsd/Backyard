#! python3
#  coding=utf-8

data = (
    (((3,17),(2,12)),((15),(25,0))),
    (((2,5),(3)),((2,14)))
)
INFI = 256

def minimax(t, depth, alpha, beta):
    if hasattr(t,'__iter__'):
        if depth%2:
            ret = INFI
            for o in t:
                r,a,b = minimax(o, depth+1, alpha, beta)
                beta = min(beta, r)
                if alpha >= beta:
                    ret = beta
                    break
                ret = min(ret, r)
        else:
            ret = -INFI
            for x in t:
                r,a,b = minimax(x, depth+1, alpha, beta)
                alpha = max(alpha, r)
                if alpha >= beta:
                    ret = alpha
                    break
                ret = max(ret, r)
    else:
        ret = t
    #print(ret, alpha, beta)
    return ret, alpha, beta

print(minimax(data, 0, -INFI, INFI))
