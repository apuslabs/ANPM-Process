# LLD

1. **文档结构**
    - 严格按照HLD的模块划分
    - 关键流程要与HLD保持一致
    - 流程图、时序图
    - 流程图、时序图
2. **模块内部结构**
    - **模块描述**: 简要说明模块的功能和作用。
    - **数据结构**: 定义模块内部使用的数据结构，包括数据库表、枚举类型、消息格式等。
    - **API 接口**: 定义模块提供的 API 接口，包括接口名称、请求参数、响应格式等。
    - **状态机**: 如果模块涉及状态变化，需要定义状态机，描述状态转换条件和动作。
        - 使用 Mermaid 语法绘制。
    - **流程图/时序图**: 展示模块与其他模块之间的交互流程。
        - 不得简化角色
        - 内部流程要表现出来
        - 权限验证、错误处理要考虑

# 命令规范

**命名风格规范说明**

- **`camelCase`**: 首字母小写，后续单词首字母大写。
    - **示例**: **`userWalletAddress`**, **`taskRef`**, **`calculateInterest`**
- **`PascalCase`**: 首字母大写，后续单词首字母大写。
    - **示例**: **`GetCreditBalance`**, **`AddTask`**, **`TaskStatus`**
- **`UPPER_SNAKE_CASE`**: 全部大写字母，单词以下划线 **`_`** 连接。
    - **示例**: **`MAX_POOL_CAPACITY`**, **`CREDIT_EXCHANGE_RATE`**, **`TASK_STATUS_PENDING`**
- **`snake_case`**: 全部小写字母，单词以下划线 **`_`** 连接。
    - **示例**: **`user_wallet_address`**, **`created_at`**, **`task_status`**
- **`kebab-case`**: 全部小写字母，单词以连字符  连接。
    - **示例**: **`add-credit`**, **`credit-balance`**, **`get-pending-task`**
- **`Pascal-Case`**: 首字母大写，后续单词首字母大写。
    - **示例**: **`Get-Credit-Balance`**, **`Add-Task`**, **`Task-Status`**

**命令规范列**

- **变量名**: **`snake_case`**
- **函数/方法名**: **`PascalCase`**
- **常量名**: **`UPPER_SNAKE_CASE`**
- **枚举值**: **`UPPER_SNAKE_CASE`**
- **消息/事件名**: **`kebab-case`**
- **API 接口名/路径**: **`kebab-case`**
- **配置项名**: **`kebab-case`**
- **数据库表名**: **`snake_case`**
- **数据库字段名**: **`snake_case`**

**AO**

- **Handler Action:** `Pascal-Case`
- **Handler Tag**: `Pascal-Case`
- **Data**: `snake_case`

**Handler**

- `Handler name`对应Handlers.add的第一个参数
- `Handler patter`对应Handlers.add的第二个参数
- 面向用户提供的接口必须使用 `msg.reply({ code: <http_code> })`

**缩写使用规范**

- **通用原则**: 避免不必要的缩写。如果单词是命名中的核心部分且缩写不常见或可能引起歧义，则应使用完整单词。
- **常见且无歧义的缩写**: 如果缩写是行业内或团队内广泛认可且含义清晰的，可以使用。例如：**`id`** (Identifier), **`ref`** (Reference), **`idx`** (Index), **`url`** (Uniform Resource Locator), **`api`** (Application Programming Interface)。
- **一致性**: 一旦决定使用某个缩写，应在整个项目中保持一致。
- **避免**: 避免使用只有项目内部才懂的、非标准的缩写。
- **示例**:
    - 推荐: **`user_id`**, **`task_ref`**, **`api_url`**
    - 不推荐: **`usrId`** (除非 **`id`** 是标准缩写且 **`usr`** 是公认缩写), **`taskNo`** (除非明确 **`No`** 代表 **`Number`** 且团队约定)

**时序图规范**

- 发送消息使用 `Send "Message Action Name"` ，不需要标传递参数，传递参数在Handler定义中使用
- 返回消息使用虚线