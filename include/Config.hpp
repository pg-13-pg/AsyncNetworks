#pragma once

#include <chrono>
#include <string>
#include <unordered_map>

class Config
{
  public:
    bool loadFromFile(const std::string &path, std::string *error = nullptr);       // 从指定文件加载配置
    bool loadFromString(const std::string &content, std::string *error = nullptr); // 从字符串内容加载配置

    bool has(const std::string &key) const; // 检查指定配置项是否存在

    std::string getString(const std::string &key, const std::string &defaultValue = "") const; // 读取字符串配置
    int getInt(const std::string &key, int defaultValue = 0) const;                         // 读取有符号整数配置
    size_t getSizeT(const std::string &key, size_t defaultValue = 0) const;                 // 读取非负整数配置
    bool getBool(const std::string &key, bool defaultValue = false) const;                  // 读取布尔配置
    std::chrono::milliseconds getDurationMs(const std::string &key,
                                            std::chrono::milliseconds defaultValue) const; // 读取毫秒时长配置

    const std::unordered_map<std::string, std::string> &all() const // 获取全部原始键值配置
    {
        return values_;
    }

  private:
    static std::string trim(const std::string &s);                                      // 去除字符串首尾空白字符
    static std::string stripComment(const std::string &line);                          // 去除配置行中的注释内容
    static bool parseBool(const std::string &s, bool &value);                          // 将字符串解析为布尔值
    static bool parseInt(const std::string &s, int &value);                            // 将字符串解析为有符号整数
    static bool parseSizeT(const std::string &s, size_t &value);                       // 将字符串解析为非负整数
    static bool parseDurationMs(const std::string &s, std::chrono::milliseconds &value); // 将字符串解析为毫秒时长

    std::unordered_map<std::string, std::string> values_; // 保存解析后的原始配置键值对
};
