from PyQt5 import QtWidgets, QtCore
from unone_ui import Ui_MainWindow

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.actionNew.triggered.connect(self.filenew)
        self.ui.actionOpen.triggered.connect(self.filexit)
        self.ui.actionAbout.triggered.connect(self.about)
        self.setWindowTitle(self.tr('Pyside'))

    def filenew(self):
        print('static method')
        QtWidgets.QApplication.quit()

    def about(self):
        print('qApp method')
        QtWidgets.qApp.quit()

    def filexit(self):
        print('instance()')
        QtWidgets.QApplication.instance().quit()

if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)

    tr = QtCore.QTranslator()
    if tr.load('main.zh_CN'):
        app.installTranslator(tr)

    mwnd = MainWindow()
    mwnd.show()

    sys.exit(app.exec_())
