#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: generator.py
@time: 2016/12/13 17:03
"""
from check.check_anhui_worker import CheckAnHuiWorker
from check.check_beijing_worker import CheckBeiJingWorker
from check.check_chongqing_worker import CheckChongQingWorker
from check.check_fujian_worker import CheckFuJianWorker
from check.check_gansu_worker import CheckGanSuWorker
from check.check_gsxt_worker import CheckGsxtWorker
from check.check_guangdong_worker import CheckGuangDongWorker
from check.check_guangxi_worker import CheckGuangXiWorker
from check.check_guizhou_worker import CheckGuiZhouWorker
from check.check_hainan_worker import CheckHaiNanWorker
from check.check_hebei_worker import CheckHeBeiWorker
from check.check_heilongjiang_worker import CheckHeiLongJiangWorker
from check.check_henan_worker import CheckHeNanWorker
from check.check_hubei_worker import CheckHuBeiWorker
from check.check_hunan_worker import CheckHuNanWorker
from check.check_jiangsu_worker import CheckJiangSuWorker
from check.check_jiangxi_worker import CheckJiangXiWorker
from check.check_jilin_worker import CheckJiLinWorker
from check.check_liaoning_worker import CheckLiaoNingWorker
from check.check_neimenggu_worker import CheckNeiMengGuWorker
from check.check_ningxia_worker import CheckNingXiaWorker
from check.check_qinghai_worker import CheckQingHaiWorker
from check.check_shandong_worker import CheckShanDongWorker
from check.check_shanghai_worker import CheckShangHaiWorker
from check.check_shanxi_worker import CheckShanXiWorker
from check.check_shanxicu_worker import CheckShanXiCuWorker
from check.check_sichuan_worker import CheckSiChuanWorker
from check.check_tianjin_worker import CheckTianJinWorker
from check.check_xinjiang_worker import CheckXinJiangWorker
from check.check_xizang_worker import CheckXiZangWorker
from check.check_yunnan_worker import CheckYunNanWorker
from check.check_zhejiang_worker import CheckZheJiangWorker

__all__ = ['GsxtHuNanWorker', 'GsxtShanXiWorker', 'GsxtNingXiaWorker', 'GsxtGuiZhouWorker',
           'GsxtLiaoNingWorker', 'GsxtJiangSuWorker', 'GsxtNeiMengGuWorker', 'GsxtTianJinWorker',
           'GsxtShanXiCuWorker', 'GsxtChongQingWorker', 'GsxtSiChuanWorker', 'GsxtXinJiangWorker',
           'GsxtShangHaiWorker', 'GsxtFuJianWorker', 'GsxtGuangDongWorker', 'GsxtBeiJingWorker',
           'GsxtHeiLongJiangWorker', 'GsxtAnHuiWorker', 'GsxtShanDongWorker', 'GsxtGuangXiWorker',
           'GsxtXiZangWorker', 'GsxtQingHaiWorker', 'GsxtHuBeiWorker', 'GsxtHeNanWorker',
           'GsxtHaiNanWorker', 'GsxtGanSuWorker',
           'GsxtDetailAnHuiWorker',
           'GsxtDetailLiaoNingWorker', 'GsxtDetailShanXiCuWorker', 'GsxtDetailShanDongWorker',
           'GsxtDetailBeiJingWorker', 'GsxtDetailXiZangWorker', 'GsxtDetailQingHaiWorker',
           'GsxtDetailHeiLongJiangWorker', 'GsxtDetailHeNanWorker', 'GsxtDetailHuBeiWorker',
           'GsxtHeBeiWorker', 'GsxtYunNanWorker', 'GsxtSearchListBeiJingWorker', 'GsxtSearchListShangHaiWorker',
           'GsxtSearchListGuangDongWorker', 'GsxtSearchListAnHuiWorker', 'GsxtDetailShangHaiWorker',
           'GsxtDetailGuangDongWorker',
           'GsxtSearchListLiaoNingWorker', 'GsxtSearchListShanDongWorker', 'GsxtSearchListShanXiCuWorker',
           'GsxtSearchListFuJianWorker', 'GsxtDetailFuJianWorker', 'GsxtSearchListHeNanWorker',
           'GsxtDetailHuNanWorker', 'GsxtSearchListHuNanWorker', 'GsxtSearchListHuBeiWorker',
           'GsxtSearchListTianJinWorker', 'GsxtSearchListHeBeiWorker', 'GsxtSearchListNeiMengGuWorker',
           'GsxtJiLinWorker', 'GsxtSearchListJiLinWorker', 'GsxtSearchListHeiLongJiangWorker',
           'GsxtSearchListJiangSuWorker', 'GsxtZheJiangWorker', 'GsxtSearchListZheJiangWorker',
           'GsxtJiangXiWorker', 'GsxtSearchListGuangXiWorker', 'GsxtSearchListHaiNanWorker',
           'GsxtSearchListJiangXiWorker', 'GsxtSearchListChongQingWorker', 'GsxtSearchListSiChuanWorker',
           'GsxtSearchListGuiZhouWorker', 'GsxtSearchListYunNanWorker', 'GsxtSearchListXiZangWorker',
           'GsxtSearchListShanXiWorker', 'GsxtSearchListGanSuWorker', 'GsxtSearchListQingHaiWorker',
           'GsxtSearchListNingXiaWorker', 'GsxtSearchListXinJiangWorker', 'GsxtDetailJiangSuWorker',
           'GsxtDetailZheJiangWorker', 'GsxtDetailSiChuanWorker', 'GsxtDetailJiangXiWorker',
           'GsxtDetailTianJinWorker', 'GsxtDetailShanXiWorker', 'GsxtDetailChongQingWorker',
           'GsxtDetailYunNanWorker', 'GsxtDetailHeBeiWorker', 'GsxtDetailXinJiangWorker',
           'GsxtDetailGuiZhouWorker', 'GsxtDetailGuangXiWorker', 'GsxtDetailHaiNanWorker',
           'GsxtDetailGanSuWorker', 'GsxtDetailJiLinWorker', 'GsxtDetailNeiMengGuWorker',
           'GsxtDetailNingXiaWorker', 'CheckAnHuiWorker', 'CheckBeiJingWorker', 'CheckChongQingWorker',
           'CheckFuJianWorker', 'CheckGanSuWorker', 'CheckGuangDongWorker', 'CheckGuangXiWorker',
           'CheckGuiZhouWorker', 'CheckHaiNanWorker', 'CheckHeBeiWorker', 'CheckHeiLongJiangWorker',
           'CheckHeNanWorker', 'CheckHuBeiWorker', 'CheckHuNanWorker', 'CheckJiangSuWorker',
           'CheckJiangXiWorker', 'CheckJiLinWorker', 'CheckLiaoNingWorker', 'CheckNeiMengGuWorker',
           'CheckNingXiaWorker', 'CheckQingHaiWorker', 'CheckShanDongWorker', 'CheckShangHaiWorker',
           'CheckShanXiWorker', 'CheckShanXiCuWorker', 'CheckSiChuanWorker', 'CheckTianJinWorker',
           'CheckXinJiangWorker', 'CheckXiZangWorker', 'CheckYunNanWorker', 'CheckZheJiangWorker',
           'CheckGsxtWorker',
           ]


def create_check_object(config_dict, province, log):
    clazz = config_dict.get('check_clazz', '')
    if clazz == '':
        return None

    return eval(clazz)(province, log)
