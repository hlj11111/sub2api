# 发布和部署自定义镜像

源码仓库负责构建；现有生产 Compose 项目继续管理数据库、Redis、配置和数据目录。
不要用源码目录下的全套 Compose 配置替换现有生产项目。

## GitHub 发布

1. 为本仓库设置 Actions repository variable `SIMPLE_RELEASE=true`。
2. 提交并推送需要发布的代码。
3. 创建并推送独立版本标签，例如 `v0.2.9-custom.1`。
4. 标签自动触发 Release workflow；成功后镜像为
   `ghcr.io/<仓库所有者的小写名称>/sub2api:0.2.9-custom.1`。
   镜像标签不含开头的 `v`。仅推送 main 不会发布镜像。
5. 首次验证可手动运行 Release：ref 填 `main`，勾选 `simple_release` 和
   `dry_run`。试构建不会发布镜像，也不会部署到服务器。

Actions 使用自身的 `GITHUB_TOKEN` 发布镜像。私有镜像由服务器使用具有
`read:packages` 权限的 classic PAT 拉取。凭据只能存放在仓库外，不提交到 Git。

## 生产切换

1. 拉取指定版本，核对架构、版本和源提交，记录镜像 digest。
2. 记录当前容器的镜像 ID，并为旧镜像增加本机回退标签。
3. 用 `pg_dump -Fc` 备份数据库；保存生产 `.env`、Compose 文件和完整应用数据目录。
   运行中的 PostgreSQL 数据目录直接打包不能代替数据库备份。
4. 在独立且禁止外部访问的 Docker 网络中恢复数据库副本，运行新镜像验证。
   使用独立 Redis、应用数据副本和数据库；不连接生产网络，避免重复执行真实任务。
5. 检查迁移、数据完整性、健康检查和前端页面。失败时不得切换。
6. 在维护窗口停止应用写入，保留 PostgreSQL 和 Redis 运行，完成最终备份。
7. 在生产部署目录创建 `docker-compose.override.yml`，仅覆盖应用镜像：

   ```yaml
   services:
     sub2api:
       image: ghcr.io/<owner>/sub2api@sha256:<verified-digest>
   ```

   项目原有 override 文件若存在，应保留并合并其配置，不得直接覆盖。
   用 `docker compose config --format json` 在内存中比对合并前后的配置，
   确保除了应用镜像外无变化；输出可能包含密码，不要打印或提交完整配置。

8. 在原生产目录运行 `docker compose up -d --no-deps --pull never sub2api`。
   默认命令会自动读取上述 override。若使用显式 `-f`，必须同时指定基础文件和
   override，不能只指定基础文件，否则会重新使用原镜像。
9. 确认应用健康、实际版本、历史用户/账号/API Key/余额/用量数据和真实业务请求。
   PostgreSQL、Redis 的容器 ID、配置及数据挂载应保持不变。

`docker-compose.custom-image.yml` 是可复用的参数化覆盖模板。手动使用时设置
`SUB2API_CUSTOM_IMAGE`，并在 `-f` 中同时指定生产基础文件和这个模板。
生产常驻配置应记录已验证的镜像 digest，避免每次依赖交互式 shell 的变量。

## 回退

保留旧镜像、最终备份和切换前的 Compose 配置。确认数据库未发生不兼容变更时，
将 override 中的镜像改为记录的旧镜像，重新创建应用容器。
如果执行了新的迁移或存在不兼容数据变更，先停止应用，再评估数据库恢复方案；
恢复旧备份会丢弃备份之后的写入，不能自动或盲目执行。

不要通过 `down -v`、删除数据目录、重新初始化 `.env` 或清理旧镜像来升级应用。
应用镜像切换会短暂中断请求，容器启动成功不等于所有上游调用均已验证。
