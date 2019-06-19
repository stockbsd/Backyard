from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import hashlib
import time

ep = (int)(time.time()) - 24 * 60 * 60 * 9

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
    return ret 

fnames = [name for (name, tp) in getFriend('WCDB_Contact.sqlite') ]

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
print(len(fnames) , len(fnamesmeta), set(fnamesmeta) - set(fnames))

fs = { hashlib.md5(fn.encode('utf-8')).hexdigest().lower():fn for fn in fnames 
        #if not (fn[-9:] != '@chatroom' and fn[:3] != 'gh_')
     }

#print(fnames)

for C in Base.classes:
    if C.__table__.name.startswith('Chat_'): # and C.__table__.name[5:] in fs:
        extname = C.__table__.name[5:]
        uname = fs.get(extname, extname)
        if uname[:3] == 'gh_' or uname == 'newsapp' or uname[-9:] == '@chatroom':
        #if uname[-9:] != '@chatroom':
            continue

        print('==============={}==============\n'.format(uname))
        #msgs = session.query(C).filter(C.Type == 1).order_by(C.CreateTime.desc()).limit(5)
        msgs = session.query(C).filter(C.Type.in_([1,34,50, 3,43,62])).filter(C.CreateTime>ep).order_by(C.CreateTime.desc()).limit(40)
        for m in (msgs):
            print(time.ctime(m.CreateTime)[:-4] + ('->' if m.Des else '  ') +  m.Message)

session.close()
