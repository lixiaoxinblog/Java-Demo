package com.xiaoxin.hotel.service.impl;

import com.xiaoxin.hotel.mapper.HotelMapper;
import com.xiaoxin.hotel.pojo.Hotel;
import com.xiaoxin.hotel.service.IHotelService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
}
