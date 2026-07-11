# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'untitled.ui'
##
## Created by: Qt User Interface Compiler version 6.11.0
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QAction, QBrush, QColor, QConicalGradient,
    QCursor, QFont, QFontDatabase, QGradient,
    QIcon, QImage, QKeySequence, QLinearGradient,
    QPainter, QPalette, QPixmap, QRadialGradient,
    QTransform)
from PySide6.QtWidgets import (QApplication, QHBoxLayout, QHeaderView, QListView,
    QMainWindow, QMenu, QMenuBar, QSizePolicy,
    QStatusBar, QTableView, QWidget)

class Ui_main_window(object):
    def setupUi(self, main_window):
        if not main_window.objectName():
            main_window.setObjectName(u"main_window")
        main_window.resize(1099, 859)
        self.actionOpen_db = QAction(main_window)
        self.actionOpen_db.setObjectName(u"actionOpen_db")
        self.actionExit = QAction(main_window)
        self.actionExit.setObjectName(u"actionExit")
        self.actionOptions = QAction(main_window)
        self.actionOptions.setObjectName(u"actionOptions")
        self.centralwidget = QWidget(main_window)
        self.centralwidget.setObjectName(u"centralwidget")
        self.tableView = QTableView(self.centralwidget)
        self.tableView.setObjectName(u"tableView")
        self.tableView.setGeometry(QRect(10, 10, 301, 801))
        font = QFont()
        font.setPointSize(10)
        self.tableView.setFont(font)
        self.listView = QListView(self.centralwidget)
        self.listView.setObjectName(u"listView")
        self.listView.setGeometry(QRect(740, 290, 341, 161))
        self.horizontalLayoutWidget = QWidget(self.centralwidget)
        self.horizontalLayoutWidget.setObjectName(u"horizontalLayoutWidget")
        self.horizontalLayoutWidget.setGeometry(QRect(330, 290, 401, 221))
        self.main_layout = QHBoxLayout(self.horizontalLayoutWidget)
        self.main_layout.setObjectName(u"main_layout")
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayoutWidget_2 = QWidget(self.centralwidget)
        self.horizontalLayoutWidget_2.setObjectName(u"horizontalLayoutWidget_2")
        self.horizontalLayoutWidget_2.setGeometry(QRect(760, 10, 331, 271))
        self.triplayout = QHBoxLayout(self.horizontalLayoutWidget_2)
        self.triplayout.setObjectName(u"triplayout")
        self.triplayout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayoutWidget_3 = QWidget(self.centralwidget)
        self.horizontalLayoutWidget_3.setObjectName(u"horizontalLayoutWidget_3")
        self.horizontalLayoutWidget_3.setGeometry(QRect(370, 520, 721, 201))
        self.entrieslayout = QHBoxLayout(self.horizontalLayoutWidget_3)
        self.entrieslayout.setObjectName(u"entrieslayout")
        self.entrieslayout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayoutWidget_4 = QWidget(self.centralwidget)
        self.horizontalLayoutWidget_4.setObjectName(u"horizontalLayoutWidget_4")
        self.horizontalLayoutWidget_4.setGeometry(QRect(330, 10, 411, 271))
        self.speedlayout = QHBoxLayout(self.horizontalLayoutWidget_4)
        self.speedlayout.setObjectName(u"speedlayout")
        self.speedlayout.setContentsMargins(0, 0, 0, 0)
        main_window.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(main_window)
        self.menubar.setObjectName(u"menubar")
        self.menubar.setGeometry(QRect(0, 0, 1099, 22))
        self.menuFile = QMenu(self.menubar)
        self.menuFile.setObjectName(u"menuFile")
        self.menuView = QMenu(self.menubar)
        self.menuView.setObjectName(u"menuView")
        self.menuAbout = QMenu(self.menubar)
        self.menuAbout.setObjectName(u"menuAbout")
        main_window.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(main_window)
        self.statusbar.setObjectName(u"statusbar")
        main_window.setStatusBar(self.statusbar)

        self.menubar.addAction(self.menuFile.menuAction())
        self.menubar.addAction(self.menuView.menuAction())
        self.menubar.addAction(self.menuAbout.menuAction())
        self.menuFile.addAction(self.actionOpen_db)
        self.menuFile.addAction(self.actionExit)
        self.menuView.addAction(self.actionOptions)

        self.retranslateUi(main_window)

        QMetaObject.connectSlotsByName(main_window)
    # setupUi

    def retranslateUi(self, main_window):
        main_window.setWindowTitle(QCoreApplication.translate("main_window", u"main_window", None))
        self.actionOpen_db.setText(QCoreApplication.translate("main_window", u"&Open db", None))
# if QT_CONFIG(shortcut)
        self.actionOpen_db.setShortcut(QCoreApplication.translate("main_window", u"Ctrl+O", None))
# endif // QT_CONFIG(shortcut)
        self.actionExit.setText(QCoreApplication.translate("main_window", u"E&xit", None))
# if QT_CONFIG(shortcut)
        self.actionExit.setShortcut(QCoreApplication.translate("main_window", u"Ctrl+Q", None))
# endif // QT_CONFIG(shortcut)
        self.actionOptions.setText(QCoreApplication.translate("main_window", u"&Options", None))
        self.menuFile.setTitle(QCoreApplication.translate("main_window", u"&File", None))
        self.menuView.setTitle(QCoreApplication.translate("main_window", u"&View", None))
        self.menuAbout.setTitle(QCoreApplication.translate("main_window", u"&About", None))
    # retranslateUi

