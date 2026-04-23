# Git 推送说明

本项目本地目录：`D:\graduate`

远程仓库：`ssh://git@ssh.github.com:443/3049083424-yuyu/bishe.git`

## 一次性确认

先进入项目目录：

```powershell
cd "D:\graduate"
```

确认远程地址：

```powershell
git remote -v
```

如果远程不是下面这个地址，就改成它：

```powershell
git remote set-url origin "ssh://git@ssh.github.com:443/3049083424-yuyu/bishe.git"
```

为避免 Git 调用错误的 SSH 客户端，固定使用 Windows OpenSSH：

```powershell
git config core.sshCommand "C:/Windows/System32/OpenSSH/ssh.exe"
```

## 每次推送新内容的步骤

先进入项目目录：

```powershell
cd "D:\graduate"
```

查看变更：

```powershell
git status
```

添加全部变更：

```powershell
git add .
```

提交变更：

```powershell
git commit -m "写这里：本次修改说明"
```

推送到远程仓库：

```powershell
git push origin main
```

## 首次连接 SSH 主机时的提示

如果终端出现下面这种提示：

```text
Are you sure you want to continue connecting (yes/no/[fingerprint])?
```

输入：

```text
yes
```

然后回车。

## 常见问题

### 1. `Permission denied (publickey)`

先测试 SSH 登录：

```powershell
ssh -o StrictHostKeyChecking=accept-new -T -p 443 git@ssh.github.com
```

如果能看到：

```text
Hi 3049083424-yuyu! You've successfully authenticated
```

说明 SSH key 正常。

然后重新执行：

```powershell
git config core.sshCommand "C:/Windows/System32/OpenSSH/ssh.exe"
git push origin main
```

### 2. `Failed to connect to github.com port 443`

不要再用 HTTPS 远程地址，必须使用：

```text
ssh://git@ssh.github.com:443/3049083424-yuyu/bishe.git
```

### 3. 想确认当前提交到了哪里

```powershell
git branch -vv
git remote -v
```

## 以后让我代推送时的默认规则

当用户要求“把新的内容推送到远程仓库”时，默认按以下流程操作：

1. 在 `D:\graduate` 执行 Git 命令。
2. 确认远程为 `ssh://git@ssh.github.com:443/3049083424-yuyu/bishe.git`。
3. 如有需要，使用 `git config core.sshCommand "C:/Windows/System32/OpenSSH/ssh.exe"`。
4. 执行 `git status` 检查变更。
5. 执行 `git add .`。
6. 使用简洁明确的提交信息执行 `git commit -m "..."`。
7. 执行 `git push origin main`。
8. 如果出现交互式 SSH 主机确认，输入 `yes`。
