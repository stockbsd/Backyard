from PyQt5 import QtWidgets
from unone_ui import Ui_MainWindow

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    mwnd = MainWindow()
    mwnd.show()
    sys.exit(app.exec_())
