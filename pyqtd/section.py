#!python3
# -*- coding: utf-8 -*-
from PyQt5 import QtCore, QtGui, QtWidgets
import math

#生成穿过source点序列的连续平滑bezier线
def buildBesier(source, scale=0.6):
    dest = [source[0], source[0]]
    for i in range(1, len(source)-1):
        mid1 = (source[i-1]+source[i])/2.0
        mid2 = (source[i+1]+source[i])/2.0
        v1 = source[i] - source[i-1]
        v2 = source[i+1] - source[i]
        len1 = (v1.x()**2 + v1.y()**2)**0.5
        len2 = (v2.x()**2 + v2.y()**2)**0.5
        t = len1/(len1+len2)    #bug
        lpCtrl = (mid1 - mid2)*t*scale + source[i]
        rpCtrl = (mid2 - mid1)*(1-t)*scale + source[i]
        dest.append(lpCtrl)
        dest.append(source[i])
        dest.append(rpCtrl)
    dest.append(source[-1])
    dest.append(source[-1])
    return dest
           
#井
class Well(QtWidgets.QGraphicsItem):
    def __init__(self, name, x, ys):
        super().__init__()
        self.name = name
        self.x = x
        self.ys = ys
        self.width = 20.0
        self.yscale = 200.0
        self.fontsize = 12.0
        
    def boundingRect(self):
        return QtCore.QRectF(-self.width/2, 0, self.width,
                (self.ys[-1]-self.ys[0])/self.yscale)
                
    def paint(self, painter, option, widget):
        painter.setPen(QtGui.QPen(QtCore.Qt.black, 0))
        painter.drawRect(self.boundingRect())
        
        font = painter.font()
        font.setPointSize(self.fontsize)
        painter.setFont(font)
        for i in range(len(self.ys)-1):
            textRect = QtCore.QRectF(-self.width/2.0, (self.ys[i]-self.ys[0])/self.yscale, 
                                     self.width, (self.ys[i+1]-self.ys[i])/self.yscale)
            painter.drawText(textRect, "%s-%i"%(self.name, i))
    
    def pts2Item(self, item):
        return [self.mapToItem(item, 0, (y-self.ys[0])/self.yscale) for y in self.ys]

# 井间
class SecWS(QtWidgets.QGraphicsItem):
    def __init__(self, ws, drawM=1):
        super().__init__()
        self.ws = ws
        self.drawM = drawM
        
    def paint(self, painter, option, widget):
        painter.setPen(QtGui.QPen(QtCore.Qt.blue, 1))
        lines = zip(*[w.pts2Item(self) for w in self.ws])
        for l in lines:
            if self.drawM==2:
                path = QtGui.QPainterPath()
                dst = buildBesier(l, 0.6)
                path.moveTo(dst[0])
                for i in range(1, len(dst), 3):
                    path.cubicTo(dst[i], dst[i+1], dst[i+2])
                painter.drawPath(path)
                #painter.drawPoints(*dst)
            elif self.drawM==1:
                path = QtGui.QPainterPath()
                path.moveTo(l[0])
                for i in range(1, len(l)):
                    dx = (l[i].x()-l[i-1].x())/3.0
                    path.cubicTo(l[i-1].x() + dx, l[i-1].y(), l[i].x()-dx, l[i].y(), l[i].x(), l[i].y())
                painter.drawPath(path)
            else:
                painter.drawPolyline(*l)
        
        painter.setPen(QtGui.QPen(QtCore.Qt.red, 0))
        painter.drawRect(self.scene().sceneRect())
    
    def boundingRect(self):
        return self.scene().sceneRect() #QtCore.QRectF(0, 0, 0, 0)

class SecView(QtWidgets.QGraphicsView):
    def __init__(self, cw, depAlign=False, drawM=1):
        super().__init__()
            
        scene = QtWidgets.QGraphicsScene(self)
        self.setScene(scene)
        self.setCacheMode(QtWidgets.QGraphicsView.CacheBackground)
        self.setViewportUpdateMode(QtWidgets.QGraphicsView.BoundingRectViewportUpdate)
        self.setRenderHint(QtGui.QPainter.Antialiasing)
        self.setRenderHint(QtGui.QPainter.TextAntialiasing)
        self.setTransformationAnchor(QtWidgets.QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QtWidgets.QGraphicsView.AnchorViewCenter)
        self.setWindowTitle('剖面图自动适应')
        self.setAttribute(QtCore.Qt.WA_DeleteOnClose) 
        
        left = min(w[0] for w in cw)
        top = min(w[1][0] for w in cw)
        xl = max(w[0] for w in cw) - left
        yl = (max(w[1][-1] for w in cw) - top
            if depAlign else max(w[1][-1]-w[1][0] for w in cw))
        bestsize = self.bestSize(xl, yl, len(cw))
        bestscale = self.bestScale(xl, yl, bestsize)
        bestww = self.bestWellWidth(bestsize[0], len(cw))
        bestfs = self.bestFontsize(bestww, 0, 0)
        print(xl, yl, bestsize, bestscale, bestww, bestfs)
        
        # 生成井
        self.ws = [Well("W%i"%i, *winfo) for i, winfo in enumerate(cw)] 
        for w in self.ws:
            scene.addItem(w) 
            w.yscale = bestscale[1]
            w.width = bestww
            w.fontsize = bestfs
            w.setPos((w.x - left)/bestscale[0], (w.ys[0]-top)/bestscale[1] if depAlign else 0)
        # 生成井间
        secws = SecWS(self.ws, drawM) #井间
        secws.setPos(0, 0)
        scene.addItem(secws) 
        # 图纸大小
        scene.setSceneRect(-25, -10, bestsize[0]+50, bestsize[1]+20)
        self.setMinimumSize(self.sceneRect().width()+20, self.sceneRect().height()+20)

    def wheelEvent(self, event):
        self.scaleView(math.pow(2.0, -event.angleDelta().y()/360.0))
    def scaleView(self, scaleFactor):
        factor = self.transform().scale(scaleFactor, scaleFactor).mapRect(QtCore.QRectF(0, 0, 1, 1)).width()
        if factor > 0.07 and factor < 100:
            self.scale(scaleFactor, scaleFactor) 
        
    # 选择图纸大小
    def bestSize(self, xlen, ylen, num):
        return (1200 if num>9 else(960 if num>6 else (750 if num>3 else 600)), 600)
    # 选择映射比例
    def bestScale(self, xl, yl, pagesize):
        return (xl/pagesize[0], yl/pagesize[1])
    # 选择井柱宽度
    def bestWellWidth(self, xsize, num):
        return min(50 if num <5 else 32, xsize/(3.0 if num<7 else 1.25)/num)
    # 选择井内字体
    def bestFontsize(self, width, mean, middle):
        return 14 if width >36 else 11
    
if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    scr = app.primaryScreen()
    print(scr.devicePixelRatio(), scr.size(), scr.physicalSize(), scr.logicalDotsPerInch(), scr.physicalDotsPerInch())

    mul = 2.0
    cwo = [(100.*mul, [100., 135., 162.0, 230., 342., 370.]),
        (200.*mul, [110., 135., 163.0, 190., 234.]), 
        (270.*mul, [100., 115., 142.0, 210., 262.]), 
        (400.*mul, [230., 275., 312.0, 320., 332., 360.]), 
        (490.*mul, [170., 205., 222.0, 250., 312.]), 
        (650.*mul, [220., 305., 342.0, 390., 432.]), 
        (700.*mul, [200., 305., 332.0, 360., 402., 413.]), 
        (760.*mul, [230., 275., 312.0, 320., 332.]), 
        (800.*mul, [170., 205., 222.0, 250., 312.]), 
        (950.*mul, [236., 309., 346.0, 395., 422., 450.0]), 
        (990.*mul, [326., 389., 426.0, 445., 472., 490.0]), 
    ]
    widget = SecView(cwo[::], 1, 2)
    widget.show()

    sys.exit(app.exec_())
