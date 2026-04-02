# ==========================================
# 1. 样式表 - 暗色主题
# ==========================================
STYLESHEET_DARK = """
/* ========== 全局样式 ========== */
QWidget {
    font-family: 'Segoe UI', 'Microsoft YaHei';
    font-size: 14px;
    color: #E4E4E7;
}

/* ========== 主容器/弹窗 ========== */
#MainWindow, #DetailWindow, #DiffWindow, #AddWindow, #SettingsWindow, #ScreenshotWindow {
    background-color: #1E1E2E;
    border-radius: 12px;
    border: 1px solid #3D3D5C;
}

/* ========== 列表样式 ========== */
QListWidget { 
    background-color: transparent; 
    border: none; 
    outline: none; 
}
QListWidget::item { 
    background-color: #2A2A3C; 
    border-radius: 8px; 
    margin: 4px 8px; 
    border: 1px solid #3D3D5C;
}
QListWidget::item:hover { 
    background-color: #363650; 
    border: 1px solid #4F4F7A;
}
QListWidget::item:selected { 
    background-color: #3B4A6B; 
    border: 1px solid #3B82F6;
}

/* ========== 列表项框架 ========== */
QFrame#ItemFrame {
    background-color: #2A2A3C;
    border: none;
    border-radius: 8px;
}

/* ========== 闪烁警告框 ========== */
QFrame#FlashFrame { 
    border: 2px solid #EF4444; 
    background-color: #3D2A2A; 
    border-radius: 8px; 
}
/* ========== 常亮警告框 ========== */
QFrame#HighlightFrame { 
    border: 2px solid #EF4444; 
    background-color: #3D2A2A; 
    border-radius: 8px; 
}


/* ========== 标签样式 ========== */
QLabel#ItemTitle { 
    font-size: 14px; 
    font-weight: bold; 
    color: #F4F4F5; 
}
QLabel#ItemSubtitle { 
    font-size: 12px; 
    color: #9CA3AF; 
}
QLabel#HistoryTitle { 
    font-size: 14px; 
    font-weight: bold; 
    color: #6B7280; 
    text-decoration: line-through; 
}
QLabel#TitleLabel { 
    font-size: 16px; 
    font-weight: bold; 
    color: #F4F4F5; 
}
QLabel#DiffHeader { 
    font-size: 12px; 
    font-weight: bold; 
    color: #9CA3AF; 
    margin-bottom: 5px; 
}
QLabel#UUIDLabel { 
    color: #6B7280; 
    font-family: 'Consolas', 'Courier New', monospace; 
}

/* ========== 顶部通告切换 ========== */
QFrame#NoticeTabContainer {
    background-color: #1F2235;
    border: 1px solid #343454;
    border-radius: 10px;
}
QPushButton#NoticeTabBtn {
    background-color: transparent;
    border: none;
    color: #9CA3AF;
    font-size: 13px;
    font-weight: bold;
    padding: 6px 10px;
    border-radius: 8px;
}
QPushButton#NoticeTabBtn:hover {
    color: #E4E4E7;
}
QPushButton#NoticeTabBtn[active="true"] {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #3B82F6, stop:1 #2563EB);
    color: #FFFFFF;
}
QPushButton#NoticeTabBtn[attention="true"] {
    border: 1px solid #EF4444;
    color: #FCA5A5;
}

/* ========== 上传/更新按钮 ========== */
QPushButton#UploadBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #3B82F6, stop:1 #2563EB);
    color: white; 
    border-radius: 6px; 
    border: none; 
    font-weight: bold; 
    padding: 4px 12px;
}
QPushButton#UploadBtn:hover { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #60A5FA, stop:1 #3B82F6);
}
QPushButton#UploadBtn:pressed { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2563EB, stop:1 #1D4ED8);
}
QPushButton#UploadBtn[uploaded="true"] {
    background: #475569;
    color: #E2E8F0;
    border: 1px solid rgba(255,255,255,0.12);
}
QPushButton#UploadBtn[uploaded="true"]:disabled {
    background: #475569;
    color: #E2E8F0;
    border: 1px solid rgba(255,255,255,0.12);
}

/* ========== 导航按钮 ========== */
QPushButton#NavBtn, QPushButton#ClearBtn, QPushButton#AddBtn, QPushButton#ThemeBtn, QPushButton#MinimizeBtn { 
    background-color: #2A2A3C; 
    border: 1px solid #3D3D5C; 
    font-size: 13px; 
    color: #9CA3AF; 
    border-radius: 8px;
    padding: 6px 10px;
}
QPushButton#NavBtn:hover, QPushButton#ThemeBtn:hover, QPushButton#MinimizeBtn:hover { 
    background-color: #363650; 
    color: #E4E4E7;
    border-color: #4F4F7A;
}
QPushButton#ClearBtn:hover { 
    color: #EF4444; 
    background-color: #3D2A2A; 
    border-color: #EF4444;
}
QPushButton#AddBtn:hover { 
    color: #22C55E; 
    background-color: #2A3D2A; 
    border-color: #22C55E;
}

/* ========== 表格链接按钮 ========== */
QPushButton#TableLinkBtn {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                stop:0 rgba(79, 70, 229, 0.35),
                                stop:1 rgba(59, 130, 246, 0.18));
    border: 1px solid rgba(99, 102, 241, 0.8);
    border-left: 4px solid rgba(129, 140, 248, 0.95);
    color: #E0E7FF;
    border-top-left-radius: 18px;
    border-bottom-left-radius: 18px;
    border-top-right-radius: 10px;
    border-bottom-right-radius: 10px;
    padding: 5px 12px 5px 16px;
    margin-left: -12px;
    font-size: 12px;
}
QPushButton#TableLinkBtn:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                stop:0 rgba(99, 102, 241, 0.55),
                                stop:1 rgba(59, 130, 246, 0.28));
    border-color: rgba(165, 180, 252, 1.0);
    border-left: 4px solid rgba(191, 219, 254, 1.0);
    color: #F8FAFF;
}
QPushButton#TableLinkBtn:pressed {
    background-color: rgba(79, 70, 229, 0.5);
}
QPushButton#TableLinkBtn:disabled {
    color: #6B7280;
    border-color: #3D3D5C;
    background-color: rgba(61, 61, 92, 0.4);
}

/* ========== 设置/OCR修复按钮 ========== */
QPushButton#SettingsBtn, QPushButton#OcrInstallBtn {
    background-color: #3D3D5C;
    border: 1px solid #4F4F7A;
    font-size: 12px;
    color: #E4E4E7;
    border-radius: 6px;
    padding: 4px 8px;
}
QPushButton#SettingsBtn:hover, QPushButton#OcrInstallBtn:hover {
    background-color: #4F4F7A;
    color: #FFFFFF;
    border-color: #6366F1;
}

/* ========== 确认/取消按钮 ========== */
QPushButton#ConfirmBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #22C55E, stop:1 #16A34A);
    color: white; 
    border-radius: 6px; 
    border: none;
    font-weight: bold; 
    padding: 6px 16px; 
}
QPushButton#ConfirmBtn:hover { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4ADE80, stop:1 #22C55E);
}
QPushButton#ConfirmBtn:disabled {
    background: #3D3D5C;
    color: #6B7280;
}

QPushButton#DiffCancelBtn { 
    background-color: #3D3D5C; 
    color: #E4E4E7; 
    border-radius: 6px; 
    border: 1px solid #4F4F7A;
    font-weight: bold; 
    padding: 6px 16px; 
}
QPushButton#DiffCancelBtn:hover { 
    background-color: #4F4F7A; 
    border-color: #6366F1;
}

/* ========== 关闭按钮 ========== */
QPushButton#CloseBtn {
    background-color: #3D3D5C;
    color: #E4E4E7;
    border: 1px solid #4F4F7A;
    border-radius: 6px;
    font-size: 16px;
    font-weight: bold;
    padding: 2px 8px;
}
QPushButton#CloseBtn:hover {
    background-color: #EF4444;
    color: white;
    border-color: #EF4444;
}

QPushButton#TemplateBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #6366F1, stop:1 #4F46E5);
    color: white; 
    border-radius: 6px; 
    border: none;
    font-weight: bold; 
    padding: 6px 16px; 
}
QPushButton#TemplateBtn:hover { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #818CF8, stop:1 #6366F1);
}

/* ========== 文本输入区 ========== */
QTextEdit { 
    border: 1px solid #3D3D5C; 
    background-color: #252535; 
    border-radius: 6px; 
    padding: 8px; 
    color: #E4E4E7;
    selection-background-color: #3B82F6;
}
QTextEdit:focus {
    border-color: #6366F1;
}

QLineEdit#UUIDEdit { 
    border: 1px solid #6366F1; 
    border-radius: 6px; 
    background-color: #252535;
    color: #E4E4E7;
    padding: 4px 8px;
}

/* ========== 通用输入框样式 ========== */
QLineEdit {
    border: 1px solid #3D3D5C;
    border-radius: 6px;
    background-color: #2A2A3C;
    color: #E4E4E7;
    padding: 8px 10px;
    font-size: 13px;
}
QLineEdit:focus {
    border-color: #6366F1;
}
QLineEdit::placeholder {
    color: #6B7280;
}

/* ========== 小型确认/取消按钮 ========== */
QPushButton#OkBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #22C55E, stop:1 #16A34A);
    border-radius: 12px; 
    color: white; 
    border: none; 
}
QPushButton#OkBtn:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4ADE80, stop:1 #22C55E);
}
QPushButton#CancelBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #EF4444, stop:1 #DC2626);
    border-radius: 12px; 
    color: white; 
    border: none; 
}
QPushButton#CancelBtn:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #F87171, stop:1 #EF4444);
}

/* ========== 滚动条样式 ========== */
QScrollBar:vertical {
    background-color: #1E1E2E;
    width: 8px;
    border-radius: 4px;
}
QScrollBar::handle:vertical {
    background-color: #3D3D5C;
    border-radius: 4px;
    min-height: 30px;
}
QScrollBar::handle:vertical:hover {
    background-color: #4F4F7A;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0;
}
QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
    background: none;
}

/* ========== 菜单样式 ========== */
QMenu {
    background-color: #2A2A3C;
    border: 1px solid #3D3D5C;
    border-radius: 8px;
    padding: 4px;
}
QMenu::item {
    padding: 6px 20px;
    border-radius: 4px;
    color: #E4E4E7;
}
QMenu::item:selected {
    background-color: #3B82F6;
}

/* ========== 消息框样式 ========== */
QMessageBox {
    background-color: #1E1E2E;
    border: 1px solid #3D3D5C;
    border-radius: 12px;
}
QMessageBox QLabel {
    color: #E4E4E7;
    font-size: 14px;
    padding: 8px;
}
QMessageBox QPushButton {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #3B82F6, stop:1 #2563EB);
    color: white;
    border: none;
    border-radius: 8px;
    padding: 8px 24px;
    min-width: 80px;
    font-weight: bold;
}
QMessageBox QPushButton:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #60A5FA, stop:1 #3B82F6);
}
QMessageBox QPushButton:pressed {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2563EB, stop:1 #1D4ED8);
}
"""

# ==========================================
# 1.2 样式表 - 白色主题
# ==========================================
STYLESHEET_LIGHT = """
/* ========== 全局样式 ========== */
QWidget {
    font-family: 'Segoe UI', 'Microsoft YaHei';
    font-size: 14px;
    color: #333333;
}

/* ========== 主容器/弹窗 ========== */
#MainWindow, #DetailWindow, #DiffWindow, #AddWindow, #SettingsWindow, #ScreenshotWindow {
    background-color: #FFFFFF;
    border-radius: 12px;
    border: 1px solid #E0E0E0;
}

/* ========== 列表样式 ========== */
QListWidget { 
    background-color: transparent; 
    border: none; 
    outline: none; 
}
QListWidget::item { 
    background-color: #F8F9FA; 
    border-radius: 8px; 
    margin: 4px 8px; 
    border: 1px solid #E9ECEF;
}
QListWidget::item:hover { 
    background-color: #E9ECEF; 
    border: 1px solid #CED4DA;
}
QListWidget::item:selected { 
    background-color: #E3F2FD; 
    border: 1px solid #2196F3;
}

/* ========== 列表项框架 ========== */
QFrame#ItemFrame {
    background-color: #F8F9FA;
    border: none;
    border-radius: 8px;
}

/* ========== 闪烁警告框 ========== */
QFrame#FlashFrame { 
    border: 2px solid #F44336; 
    background-color: #FFEBEE; 
    border-radius: 8px; 
}
/* ========== 常亮警告框 ========== */
QFrame#HighlightFrame { 
    border: 2px solid #F44336; 
    background-color: #FFEBEE; 
    border-radius: 8px; 
}


/* ========== 标签样式 ========== */
QLabel#ItemTitle { 
    font-size: 14px; 
    font-weight: bold; 
    color: #212529; 
}
QLabel#ItemSubtitle { 
    font-size: 12px; 
    color: #6C757D; 
}
QLabel#HistoryTitle { 
    font-size: 14px; 
    font-weight: bold; 
    color: #ADB5BD; 
    text-decoration: line-through; 
}
QLabel#TitleLabel { 
    font-size: 16px; 
    font-weight: bold; 
    color: #212529; 
}
QLabel#DiffHeader { 
    font-size: 12px; 
    font-weight: bold; 
    color: #6C757D; 
    margin-bottom: 5px; 
}
QLabel#UUIDLabel { 
    color: #ADB5BD; 
    font-family: 'Consolas', 'Courier New', monospace; 
}

/* ========== 顶部通告切换 ========== */
QFrame#NoticeTabContainer {
    background-color: #F3F4F6;
    border: 1px solid #E5E7EB;
    border-radius: 10px;
}
QPushButton#NoticeTabBtn {
    background-color: transparent;
    border: none;
    color: #6B7280;
    font-size: 13px;
    font-weight: bold;
    padding: 6px 10px;
    border-radius: 8px;
}
QPushButton#NoticeTabBtn:hover {
    color: #111827;
}
QPushButton#NoticeTabBtn[active="true"] {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #60A5FA, stop:1 #3B82F6);
    color: #FFFFFF;
}
QPushButton#NoticeTabBtn[attention="true"] {
    border: 1px solid #EF4444;
    color: #DC2626;
}

/* ========== 上传/更新按钮 ========== */
QPushButton#UploadBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2196F3, stop:1 #1976D2);
    color: white; 
    border-radius: 6px; 
    border: none; 
    font-weight: bold; 
    padding: 4px 12px;
}
QPushButton#UploadBtn:hover { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #42A5F5, stop:1 #2196F3);
}
QPushButton#UploadBtn:pressed { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #1976D2, stop:1 #1565C0);
}
QPushButton#UploadBtn[uploaded="true"] {
    background: #E2E8F0;
    color: #475569;
    border: 1px solid rgba(71,85,105,0.2);
}
QPushButton#UploadBtn[uploaded="true"]:disabled {
    background: #E2E8F0;
    color: #475569;
    border: 1px solid rgba(71,85,105,0.2);
}

/* ========== 导航按钮 ========== */
QPushButton#NavBtn, QPushButton#ClearBtn, QPushButton#AddBtn, QPushButton#ThemeBtn, QPushButton#MinimizeBtn { 
    background-color: #F8F9FA; 
    border: 1px solid #DEE2E6; 
    font-size: 13px; 
    color: #495057; 
    border-radius: 8px;
    padding: 6px 10px;
}
QPushButton#NavBtn:hover, QPushButton#ThemeBtn:hover, QPushButton#MinimizeBtn:hover { 
    background-color: #E9ECEF; 
    color: #212529;
    border-color: #CED4DA;
}
QPushButton#ClearBtn:hover { 
    color: #F44336; 
    background-color: #FFEBEE; 
    border-color: #F44336;
}
QPushButton#AddBtn:hover { 
    color: #4CAF50; 
    background-color: #E8F5E9; 
    border-color: #4CAF50;
}

/* ========== 表格链接按钮 ========== */
QPushButton#TableLinkBtn {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                stop:0 rgba(37, 99, 235, 0.12),
                                stop:1 rgba(59, 130, 246, 0.06));
    border: 1px solid rgba(37, 99, 235, 0.45);
    border-left: 4px solid rgba(59, 130, 246, 0.7);
    color: #1E3A8A;
    border-top-left-radius: 18px;
    border-bottom-left-radius: 18px;
    border-top-right-radius: 10px;
    border-bottom-right-radius: 10px;
    padding: 5px 12px 5px 16px;
    margin-left: -12px;
    font-size: 12px;
}
QPushButton#TableLinkBtn:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                stop:0 rgba(37, 99, 235, 0.2),
                                stop:1 rgba(59, 130, 246, 0.12));
    border-color: rgba(37, 99, 235, 0.8);
    border-left: 4px solid rgba(59, 130, 246, 0.95);
    color: #1E40AF;
}
QPushButton#TableLinkBtn:pressed {
    background-color: rgba(30, 64, 175, 0.22);
}
QPushButton#TableLinkBtn:disabled {
    color: #9CA3AF;
    border-color: #E5E7EB;
    background-color: rgba(229, 231, 235, 0.5);
}

/* ========== 设置/OCR修复按钮 ========== */
QPushButton#SettingsBtn, QPushButton#OcrInstallBtn {
    background-color: #E9ECEF;
    border: 1px solid #CED4DA;
    font-size: 12px;
    color: #495057;
    border-radius: 6px;
    padding: 4px 8px;
}
QPushButton#SettingsBtn:hover, QPushButton#OcrInstallBtn:hover {
    background-color: #DEE2E6;
    color: #212529;
    border-color: #2196F3;
}

/* ========== 确认/取消按钮 ========== */
QPushButton#ConfirmBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4CAF50, stop:1 #388E3C);
    color: white; 
    border-radius: 6px; 
    border: none;
    font-weight: bold; 
    padding: 6px 16px; 
}
QPushButton#ConfirmBtn:hover { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #66BB6A, stop:1 #4CAF50);
}
QPushButton#ConfirmBtn:disabled {
    background: #E0E0E0;
    color: #9E9E9E;
}

QPushButton#DiffCancelBtn { 
    background-color: #E0E0E0; 
    color: #424242; 
    border-radius: 6px; 
    border: 1px solid #BDBDBD;
    font-weight: bold; 
    padding: 6px 16px; 
}
QPushButton#DiffCancelBtn:hover { 
    background-color: #BDBDBD; 
    border-color: #9E9E9E;
}

QPushButton#TemplateBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #7C4DFF, stop:1 #651FFF);
    color: white; 
    border-radius: 6px; 
    border: none;
    font-weight: bold; 
    padding: 6px 16px; 
}
QPushButton#TemplateBtn:hover { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #B388FF, stop:1 #7C4DFF);
}

/* ========== 文本输入区 ========== */
QTextEdit { 
    border: 1px solid #E0E0E0; 
    background-color: #FAFAFA; 
    border-radius: 6px; 
    padding: 8px; 
    color: #212529;
    selection-background-color: #2196F3;
}
QTextEdit:focus {
    border-color: #2196F3;
}

QLineEdit#UUIDEdit { 
    border: 1px solid #2196F3; 
    border-radius: 6px; 
    background-color: #FFFFFF;
    color: #212529;
    padding: 4px 8px;
}

/* ========== 通用输入框样式 ========== */
QLineEdit {
    border: 1px solid #E0E0E0;
    border-radius: 6px;
    background-color: #FFFFFF;
    color: #212529;
    padding: 8px 10px;
    font-size: 13px;
}
QLineEdit:focus {
    border-color: #2196F3;
}
QLineEdit::placeholder {
    color: #9E9E9E;
}

/* ========== 小型确认/取消按钮 ========== */
QPushButton#OkBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4CAF50, stop:1 #388E3C);
    border-radius: 12px; 
    color: white; 
    border: none; 
}
QPushButton#OkBtn:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #66BB6A, stop:1 #4CAF50);
}
QPushButton#CancelBtn { 
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #F44336, stop:1 #D32F2F);
    border-radius: 12px; 
    color: white; 
    border: none; 
}
QPushButton#CancelBtn:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #EF5350, stop:1 #F44336);
}

/* ========== 滚动条样式 ========== */
QScrollBar:vertical {
    background-color: #F5F5F5;
    width: 8px;
    border-radius: 4px;
}
QScrollBar::handle:vertical {
    background-color: #BDBDBD;
    border-radius: 4px;
    min-height: 30px;
}
QScrollBar::handle:vertical:hover {
    background-color: #9E9E9E;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0;
}
QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
    background: none;
}

/* ========== 菜单样式 ========== */
QMenu {
    background-color: #FFFFFF;
    border: 1px solid #E0E0E0;
    border-radius: 8px;
    padding: 4px;
}
QMenu::item {
    padding: 6px 20px;
    border-radius: 4px;
    color: #212529;
}
QMenu::item:selected {
    background-color: #2196F3;
    color: white;
}

/* ========== 消息框样式 ========== */
QMessageBox {
    background-color: #FFFFFF;
    border: 1px solid #E0E0E0;
    border-radius: 12px;
}
QMessageBox QLabel {
    color: #212529;
    font-size: 14px;
    padding: 8px;
}
QMessageBox QPushButton {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2196F3, stop:1 #1976D2);
    color: white;
    border: none;
    border-radius: 8px;
    padding: 8px 24px;
    min-width: 80px;
    font-weight: bold;
}
QMessageBox QPushButton:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #42A5F5, stop:1 #2196F3);
}
QMessageBox QPushButton:pressed {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #1976D2, stop:1 #1565C0);
}
"""


def get_stylesheet(theme="dark"):
    """获取指定主题的样式表"""
    if theme == "light":
        return STYLESHEET_LIGHT
    return STYLESHEET_DARK
