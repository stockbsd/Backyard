
from distutils.util import get_platform
from distutils.msvccompiler import get_build_version

from setuptools import msvc

p = get_platform()
v = get_build_version()
if v >= 14.0:
    print(p, v, msvc.msvc14_get_vc_env(p))
else:
    print(p, v, msvc.msvc9_find_vcvarsall(v))