import unittest
import os
import json
import tempfile
from utility.proxy_mana import ProxyManager
from utility.io_fctns import read_json, save_json  # ← 使用你刚给的函数


class TestProxyManager(unittest.TestCase):
    """测试 ProxyManager 的初始化、保存与随机代理功能"""

    def setUp(self):
        # 创建临时目录
        self.tempdir = tempfile.TemporaryDirectory()
        self.dpath_base_asset = self.tempdir.name

        # 创建一个模拟的 data.http.json 文件
        self.http_json_path = os.path.join(self.dpath_base_asset, 'data.http.json')
        proxyinfos = [
            {'proxy': 'http://proxy1.example.com:8080'},
            {'proxy': 'http://proxy2.example.com:8080'}
        ]
        save_json(proxyinfos, self.http_json_path)  # ✅ 使用你自己的 save_json()

        # 模拟 preset 对象
        class Preset:
            pass

        self.preset = Preset()
        self.preset.dpath_base_asset = self.dpath_base_asset

    def tearDown(self):
        # 自动清理临时目录
        self.tempdir.cleanup()

    def test_proxy_manager_workflow(self):
        """测试 ProxyManager 的完整工作流程"""
        # 读取 data.http.json（使用你的 read_json）
        proxyinfos = read_json(self.http_json_path)
        proxies = [pxinfo['proxy'] for pxinfo in proxyinfos]

        # 初始化 ProxyManager
        fpath_proxyinfo = os.path.join(self.preset.dpath_base_asset, 'proxyinfos.http.jsonl')
        pm = ProxyManager(fpath_proxyinfo)
        pm.initialize(proxies)
        pm.save()

        # 验证文件保存成功
        self.assertTrue(os.path.exists(fpath_proxyinfo), "proxyinfos.http.jsonl 文件未生成")

        # 验证随机代理输出合法
        for _ in range(10):
            proxy = pm.rand_ts_proxy_for_aiohttp()
            self.assertIn(proxy, proxies, f"返回的代理 {proxy} 不在初始化列表中")

        print("\n✅ ProxyManager 测试通过")


if __name__ == '__main__':
    unittest.main(verbosity=2)
