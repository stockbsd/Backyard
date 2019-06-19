from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import hashlib, shutil, os


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

fnames = [name for (name, tp) in getFriend('WCDB_Contact.sqlite')]


fs = { hashlib.md5(fn.encode('utf-8')).hexdigest().lower():fn for fn in fnames 
        #if not (fn[-9:] != '@chatroom' and fn[:3] != 'gh_')
     }

for root, dirs, files in os.walk('Audio'):
    for d in dirs:
        if d in fs:
            if fs[d][-9:] != '@chatroom' and fs[d][:3] != 'gh_':
                print (root, d, fs[d], 'Audio2')
                #shutil.move(os.path.join(root, d), 'Audio2')
            else:
                print (root, d, fs[d])
        else:
            #shutil.move(os.path.join(root, d), 'Audio2')
            print (root, d )

