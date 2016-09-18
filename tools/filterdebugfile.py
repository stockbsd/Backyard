import os, stat
from os import listdir, rename, walk, remove
from os.path import isfile, join

def filterdebug(src, ext='.dll'):
    #for f in listdir(mpath):
    cl = len(ext)
    for mpath, dirs, files in walk(src):
        for f in files:
            if isfile(join(mpath, f)) and f[-cl:] == ext:
                dfile = f[:-cl] + 'd' + f[-cl:]
                ddfile  = f[:-cl] + 'dd' + f[-cl:]
                if isfile(join(mpath, dfile)) and not isfile(join(mpath, ddfile)):
                    target = join(mpath, dfile)
                    print(target)

                    #os.chmod(target,stat.S_IWRITE)
                    remove(target)
                    #rename(join(mpath, dfile), join(mpath,'debug', dfile))

filterdebug(r'D:\Programs\Qt5.7\msvc2015_64\lib', '.lib')

    