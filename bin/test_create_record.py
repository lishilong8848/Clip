import sys
import os
import uuid
import logging

# 配置日志输出
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# 添加模块搜索路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

try:
    from upload_event_module.services.feishu_service import create_bitable_record
    from upload_event_module.config import config
except ImportError as e:
    print(f"导入失败，请确保在 bin 目录下运行此脚本: {e}")
    sys.exit(1)


def test_add_record():
    print("================ 测试开始 ================")

    # 1. 检查配置
    if not config.user_token:
        print("错误: 未检测到飞书 User Token，请检查配置文件 secrets.yaml")
        return

    print(f"当前 App Token: {config.app_token}")

    # 2. 准备测试数据
    test_uuid = str(uuid.uuid4())
    data_source_text = f"测试数据源内容 - {test_uuid}"
    notice_type = "事件通告"  # 可以改为 "设备变更" 或 "设备调整"

    print(f"准备创建记录: Type={notice_type}, UUID={test_uuid}")

    # 3. 调用创建接口
    success, result = create_bitable_record(
        record_uuid=test_uuid,
        data_source_text=data_source_text,
        notice_type=notice_type,
        response_time="12:00",
        buildings=["测试楼栋A", "测试楼栋B"],  # 测试多选楼栋
    )

    # 4. 输出结果
    if success:
        print(f"✅ 创建成功! Record ID: {result}")
    else:
        print(f"❌ 创建失败! Error: {result}")

    print("================ 测试结束 ================")


if __name__ == "__main__":
    test_add_record()
