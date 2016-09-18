#
import sys, os

def createPyWrapper(ericdir, wfile, isGuiScript=True, pyexedir=None):
        wname = wfile  + ".bat"
        if not pyexedir:
            pyexedir = os.path.dirname(sys.executable)

        if isGuiScript:
            wrapper = \
                '''@echo off

                SETLOCAL
                set QT_AUTO_SCREEN_SCALE_FACTOR=1
                set PYEXEDIR={0}
                set PATH=%PYEXEDIR%;%PATH%
                set PYTHONPATH={1};%PYTHONPATH%
                start "" "%PYEXEDIR%\pythonw.exe" "{1}\eric6\{2}.pyw" %1 %2 %3 %4 %5 %6 %7 %8 %9
                ENDLOCAL'''.format(pyexedir, ericdir, wfile)

        else:
            wrapper = \
                '''@echo off

                SETLOCAL
                set PYEXEDIR={0}
                set PATH=%PYEXEDIR%;%PATH%
                set PYTHONPATH={1};%PYTHONPATH%
                start "" /B /W "%PYEXEDIR%\python.exe" "{1}\eric6\{2}.py" %1 %2 %3 %4 %5 %6 %7 %8 %9
                ENDLOCAL'''.format(pyexedir, ericdir, wfile)
        with open(wname, 'w') as f:
            f.write(wrapper)

ericdir = r'd:\programs\Eric'

for name in ["eric6_api", "eric6_doc"]:
        createPyWrapper(ericdir, name, False)
for name in ["eric6_compare", "eric6_configure", "eric6_diff",
                 "eric6_editor", "eric6_iconeditor", "eric6_plugininstall",
                 "eric6_pluginrepository", "eric6_pluginuninstall",
                 "eric6_qregexp", "eric6_qregularexpression", "eric6_re",
                 "eric6_snap", "eric6_sqlbrowser", "eric6_tray",
                 "eric6_trpreviewer", "eric6_uipreviewer", "eric6_unittest",
                 "eric6_webbrowser", "eric6"]:
        createPyWrapper(ericdir, name)