#! python3
from PyQt5 import QtCore
from PyQt5 import QtWidgets

import sys
app = QtWidgets.QApplication(sys.argv)

print(app.libraryPaths())
print(app.applicationDirPath())
print(app.applicationFilePath())

print('\n')
for i in range(14):
    print(QtCore.QLibraryInfo.location(i))

