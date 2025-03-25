# FAQ 文档



---
## 如何进度Django 后台管理页面:

在`src`目录下，按照以下步骤操作

### 数据库迁移
```bash
python init.py makemigrations
python init.py migrate
```
### 创建超级用户
```bash
python init.py createsuperuser
```
按照提示输入用户名、邮箱和密码

### 启动开发服务
```bash
python init.py runserver
```

## 如何拉起单元测试？

在 `omniadvisor` 根目录下，按照以下步骤运行单元测试：

### 安装测试环境依赖
```bash
poetry install --with test
```
该命令用于安装测试环境所需的依赖。

### 激活虚拟环境
```bash
poetry shell
```

### 运行单元测试
- **多线程运行测试**：
  ```bash
  python -m pytest -n auto
  ```
- **单线程运行测试**：
  ```bash
  python -m pytest
  ```

### 退出虚拟环境
```bash
exit
```

---

## 如何进入 Django 后台管理页面？

在 `src/django_server` 目录下，按照以下步骤操作：

### 数据库迁移
```bash
python manage.py makemigrations
python manage.py migrate
```

### 创建超级用户
```bash
python manage.py createsuperuser
```
按照提示输入用户名、邮箱和密码。

### 启动开发服务器
```bash
python manage.py runserver
```

### 登录管理页面
访问以下地址以登录 Django 后台管理页面：
[http://127.0.0.1:8000/admin](http://127.0.0.1:8000/admin)
