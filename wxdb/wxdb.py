from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import hashlib
import time


def getFriend(db):
    Base = automap_base()
    engine = create_engine("sqlite:///{}".format(db))

    Base.prepare(engine, reflect=True)
    session = Session(engine)

    ret = None
    for C in Base.classes:
        if C.__table__.name == 'Friend':
            nt = session.query(C.userName, C.type)
            ret = [(n, t) for n,t in nt]  
            break

    session.close()
    '''
    for n, t in ret:
        if t & 0x3:
            if n[:3] == 'gh_' or n[-9:] == '@chatroom':
                continue

            if t & 0x8:
                print(n, '拉黑')
            if t & 0x40:
                print(n, '标星')
            if t & 0x100:
                print(n, '不看')
            if t & 0x10000:
                print(n, '不让看')
            print(n, '好友')
    '''
    return ret 


def anadb(days=5, bChat=True, bGh=False, bRoom=False, bCmb=False, bFilter=False):
    fnames = [name for (name, tp) in getFriend('WCDB_Contact.sqlite') ]
    ep = (int)(time.time()) - 24 * 60 * 60 * days

    Base = automap_base()
    engine = create_engine("sqlite:///MM.sqlite")

    # reflect the tables
    Base.prepare(engine, reflect=True)

    session = Session(engine)

    # metadata里有全部表信息，classes里只有拥有主键的表所映射的mapper类。
    fnamesmeta = []
    for t, T in Base.metadata.tables.items(): # dict
        if t == 'friend_meta':
            fnamesmeta = [name for (name,) in session.query(T.c.username)]   
            break
    #print(len(fnames) , len(fnamesmeta), set(fnamesmeta) - set(fnames))
    fnames.extend(set(fnamesmeta) - set(fnames))

    fs = { hashlib.md5(fn.encode('utf-8')).hexdigest().lower():fn for fn in fnames } 

    for C in Base.classes:
        if C.__table__.name.startswith('Chat_'): # and C.__table__.name[5:] in fs:
            extname = C.__table__.name[5:]
            uname = fs.get(extname, extname)

            if (uname[:3] == 'gh_' or uname == 'newsapp'):
                if not bGh: continue
                #mtype = 1
            elif (uname[-9:] == '@chatroom'):
                if not bRoom: continue
                #mtype = 2
            elif (uname in ['notification_messages', 'cmb4008205555']):
                if not bCmb: continue
                #mtype = 3
            else:
                if not bChat: continue
                #mtype = 4

            print('==============={}==============\n'.format(uname))
            #msgs = session.query(C).filter(C.Type == 1).order_by(C.CreateTime.desc()).limit(5)

            #filter(C.Type.in_([1,34,50, 3,43,62, 49])).
            msgs = session.query(C).filter(C.CreateTime>ep)
            if bFilter:
                msgs = msgs.filter(C.Type == 1)
            msgs = msgs.order_by(C.CreateTime.desc()) #.limit(60)
            for m in (msgs):
                print(time.ctime(m.CreateTime)[:-4] + ('->' if not m.Des else '  ') + m.Message)

    session.close()

if '__main__' == __name__:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('days', type=int, help='', default=5)
    parser.add_argument('--gh', action='store_true', help='', default=False)
    parser.add_argument('--room', action='store_true', help='', default=False)
    parser.add_argument('--cmb', action='store_true', help='', default=False)
    #parser.add_argument('--chat', action='store_true', help='', default=True)
    parser.add_argument('--nochat', dest='chat', action='store_false', help='') 
    parser.add_argument('--f', dest='filter', action='store_true', help='')
    args = parser.parse_args()
    anadb(args.days, args.chat, args.gh, args.room, args.cmb, args.filter)

