package httpapi

import (
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi/oas"
)

const (
	userIDHeader         = "X-User-Id"
	idempotencyKeyHeader = "Idempotency-Key"
	maxIdempotencyKeyLen = 64
)

const userIDKey = "user_id"

// requireUser 从 X-User-Id 取当前用户。1a 没有真实鉴权，这个头就代表已登录用户。
func (s *server) requireUser(c *gin.Context) {
	uid, ok := parseID(c.GetHeader(userIDHeader))
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "missing or invalid "+userIDHeader+" header")
		c.Abort()
		return
	}
	c.Set(userIDKey, uid)
}

func userID(c *gin.Context) int64 { return c.GetInt64(userIDKey) }

func badRequest(c *gin.Context, msg string) {
	writeError(c, http.StatusBadRequest, "INVALID_ARGUMENT", msg)
}

// bindError 处理生成代码在绑定参数时报的错（目前只有缺少必填请求头这一种），
// 保证这类错误也用统一的错误结构，而不是生成代码默认的格式。
func bindError(c *gin.Context, err error, status int) {
	writeError(c, status, "INVALID_ARGUMENT", err.Error())
}

// pathID 解析路径参数，失败时已写出 400。
// 生成代码按规范把路径里的 ID 作为字符串传进来，是否为正整数在这里校验。
func pathID(c *gin.Context, name, value string) (int64, bool) {
	n, ok := parseID(value)
	if !ok {
		badRequest(c, "invalid "+name)
	}
	return n, ok
}

// decodeBody 读取并解析 JSON 请求体：限制大小，拒绝未知字段。
// allowEmpty 为 true 时空 body 视为没有传（dst 保持零值）。失败时已写出响应。
func decodeBody(c *gin.Context, dst any, allowEmpty bool) bool {
	body := http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes)
	raw, err := io.ReadAll(body)
	if err != nil {
		if isBodyTooLarge(err) {
			writeError(c, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body too large")
		} else {
			badRequest(c, "failed to read request body")
		}
		return false
	}
	if len(raw) == 0 && allowEmpty {
		return true
	}
	if err := json.Unmarshal(raw, dst, json.RejectUnknownMembers(true)); err != nil {
		badRequest(c, "invalid JSON body")
		return false
	}
	return true
}

// CreateOrder 实现 POST /api/v1/orders。
func (s *server) CreateOrder(c *gin.Context, params oas.CreateOrderParams) {
	// 请求头缺失已被生成代码拦下，这里再校验空值和长度。
	key := params.IdempotencyKey
	if key == "" || len(key) > maxIdempotencyKeyLen {
		badRequest(c, idempotencyKeyHeader+" header is required and must be 1 to "+strconv.Itoa(maxIdempotencyKeyLen)+" characters")
		return
	}
	var req oas.CreateOrderRequest
	if !decodeBody(c, &req, false) {
		return
	}
	if len(req.Items) == 0 {
		badRequest(c, "items must not be empty")
		return
	}
	items := make([]*orderv1.SubmitOrderItem, 0, len(req.Items))
	for _, it := range req.Items {
		if it.SkuId <= 0 || it.Quantity <= 0 {
			badRequest(c, "each item needs a positive sku_id and quantity")
			return
		}
		items = append(items, &orderv1.SubmitOrderItem{SkuId: int64(it.SkuId), Quantity: it.Quantity})
	}

	resp, err := s.deps.Order.SubmitOrder(c.Request.Context(), &orderv1.SubmitOrderRequest{
		UserId:         userID(c),
		IdempotencyKey: key,
		Items:          items,
	})
	if err != nil {
		s.writeGRPCError(c, err)
		return
	}
	if resp.GetOrder() == nil {
		s.writeEmptyResponse(c, "SubmitOrder")
		return
	}
	// 下单是异步的：这里只表示请求已受理，订单还没落库。Location 告诉客户端去哪里轮询结果。
	c.Header("Location", "/api/v1/orders/"+strconv.FormatInt(resp.GetOrder().GetOrderId(), 10))
	writeJSON(c, http.StatusAccepted, toOrder(resp.GetOrder()))
}

// GetOrder 实现 GET /api/v1/orders/{order_id}。
func (s *server) GetOrder(c *gin.Context, rawOrderID oas.OrderId) {
	orderID, ok := pathID(c, "order_id", rawOrderID)
	if !ok {
		return
	}
	resp, err := s.deps.Order.GetOrder(c.Request.Context(), &orderv1.GetOrderRequest{OrderId: orderID, UserId: userID(c)})
	if err != nil {
		s.writeGRPCError(c, err)
		return
	}
	if resp.GetOrder() == nil {
		s.writeEmptyResponse(c, "GetOrder")
		return
	}
	writeJSON(c, http.StatusOK, toOrder(resp.GetOrder()))
}

// CancelOrder 实现 POST /api/v1/orders/{order_id}/cancel。
func (s *server) CancelOrder(c *gin.Context, rawOrderID oas.OrderId) {
	orderID, ok := pathID(c, "order_id", rawOrderID)
	if !ok {
		return
	}
	resp, err := s.deps.Order.CancelOrder(c.Request.Context(), &orderv1.CancelOrderRequest{OrderId: orderID, UserId: userID(c)})
	if err != nil {
		s.writeGRPCError(c, err)
		return
	}
	if resp.GetOrder() == nil {
		s.writeEmptyResponse(c, "CancelOrder")
		return
	}
	writeJSON(c, http.StatusOK, toOrder(resp.GetOrder()))
}

// PayOrder 实现 POST /api/v1/orders/{order_id}/pay。
func (s *server) PayOrder(c *gin.Context, rawOrderID oas.OrderId) {
	orderID, ok := pathID(c, "order_id", rawOrderID)
	if !ok {
		return
	}
	var req oas.PayOrderRequest
	if !decodeBody(c, &req, true) {
		return
	}
	// 没有传 outcome 和传空字符串都表示不指定。
	var requested oas.SimulatedOutcome
	if req.Outcome != nil {
		requested = *req.Outcome
	}
	var outcome paymentv1.SimulatedOutcome
	switch requested {
	case "":
		outcome = paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_UNSPECIFIED
	case oas.SimulatedOutcomeSuccess:
		outcome = paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS
	case oas.SimulatedOutcomeFailure:
		outcome = paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE
	default:
		badRequest(c, `outcome must be "success" or "failure"`)
		return
	}

	resp, err := s.deps.Payment.CreatePayment(c.Request.Context(), &paymentv1.CreatePaymentRequest{
		OrderId: orderID,
		UserId:  userID(c),
		Outcome: outcome,
	})
	if err != nil {
		s.writeGRPCError(c, err)
		return
	}
	if resp.GetPayment() == nil {
		s.writeEmptyResponse(c, "CreatePayment")
		return
	}
	// 202：支付单已受理，结果异步产生，客户端需要轮询。
	writeJSON(c, http.StatusAccepted, toPayment(resp.GetPayment()))
}

// GetPayment 实现 GET /api/v1/orders/{order_id}/payment。
func (s *server) GetPayment(c *gin.Context, rawOrderID oas.OrderId) {
	orderID, ok := pathID(c, "order_id", rawOrderID)
	if !ok {
		return
	}
	resp, err := s.deps.Payment.GetPayment(c.Request.Context(), &paymentv1.GetPaymentRequest{OrderId: orderID, UserId: userID(c)})
	if err != nil {
		s.writeGRPCError(c, err)
		return
	}
	if resp.GetPayment() == nil {
		s.writeEmptyResponse(c, "GetPayment")
		return
	}
	writeJSON(c, http.StatusOK, toPayment(resp.GetPayment()))
}

// GetStock 实现 GET /api/v1/skus/{sku_id}/stock。
func (s *server) GetStock(c *gin.Context, rawSKUID oas.SkuId) {
	skuID, ok := pathID(c, "sku_id", rawSKUID)
	if !ok {
		return
	}
	resp, err := s.deps.Inventory.GetStock(c.Request.Context(), &inventoryv1.GetStockRequest{SkuId: skuID})
	if err != nil {
		s.writeGRPCError(c, err)
		return
	}
	writeJSON(c, http.StatusOK, toStock(resp))
}

// writeEmptyResponse 处理下游成功却没带数据的情况，这违反契约，按内部错误处理。
func (s *server) writeEmptyResponse(c *gin.Context, rpc string) {
	s.writeGRPCError(c, errors.New("empty response from "+rpc))
}

const (
	defaultListLimit = 20
	maxListLimit     = 100
)

// listQuery 是两个列表接口共用的查询参数解析结果。
type listQuery struct {
	pageSize int32
	before   int64
	status   orderv1.OrderStatus
}

// parseListQuery 解析列表接口的 limit、cursor、status，失败时已写出 400。
// cursor 对客户端不透明，实际是下一页的 before_order_id（正整数的十进制字符串）。
func parseListQuery(c *gin.Context, limit *int32, cursor *string, status *oas.OrderStatus) (listQuery, bool) {
	q := listQuery{pageSize: defaultListLimit}
	if limit != nil {
		q.pageSize = *limit
		if q.pageSize < 1 || q.pageSize > maxListLimit {
			badRequest(c, "limit must be between 1 and "+strconv.Itoa(maxListLimit))
			return listQuery{}, false
		}
	}

	if cursor != nil && *cursor != "" {
		n, ok := parseID(*cursor)
		if !ok {
			badRequest(c, "invalid cursor")
			return listQuery{}, false
		}
		q.before = n
	}

	// 对外状态名到 proto 枚举；unspecified 和不传一样，表示不筛选。
	if status != nil {
		v, found := orderv1.OrderStatus_value["ORDER_STATUS_"+strings.ToUpper(string(*status))]
		if !found {
			badRequest(c, "invalid status")
			return listQuery{}, false
		}
		q.status = orderv1.OrderStatus(v)
	}
	return q, true
}

// nextCursor 把下一页的 before_order_id 编码成对外的游标；0 表示没有下一页。
func nextCursor(next int64) *string {
	if next <= 0 {
		return nil
	}
	cur := strconv.FormatInt(next, 10)
	return &cur
}

// ListOrders 实现 GET /api/v1/orders。
// 直接读订单库（只查用户所在的分片），一致性强于商家列表；已受理但还没落库的订单不在列表里。
func (s *server) ListOrders(c *gin.Context, params oas.ListOrdersParams) {
	q, ok := parseListQuery(c, params.Limit, params.Cursor, params.Status)
	if !ok {
		return
	}
	resp, err := s.deps.Order.ListUserOrders(c.Request.Context(), &orderv1.ListUserOrdersRequest{
		UserId:        userID(c),
		Status:        q.status,
		PageSize:      q.pageSize,
		BeforeOrderId: q.before,
	})
	if err != nil {
		s.writeGRPCError(c, err)
		return
	}

	out := oas.OrderSummaryList{Orders: make([]oas.OrderSummary, 0, len(resp.GetOrders()))}
	for _, o := range resp.GetOrders() {
		out.Orders = append(out.Orders, toOrderSummary(o))
	}
	out.NextCursor = nextCursor(resp.GetNextBeforeOrderId())
	writeJSON(c, http.StatusOK, out)
}

// ListMerchantOrders 实现 GET /api/v1/merchants/{merchant_id}/orders。
// 数据来自异步同步的索引，是最终一致的；cursor 对客户端不透明，实际是下一页的 before_order_id。
func (s *server) ListMerchantOrders(c *gin.Context, rawMerchantID oas.MerchantId, params oas.ListMerchantOrdersParams) {
	merchantID, ok := pathID(c, "merchant_id", rawMerchantID)
	if !ok {
		return
	}
	q, ok := parseListQuery(c, params.Limit, params.Cursor, params.Status)
	if !ok {
		return
	}

	resp, err := s.deps.OrderIndex.ListMerchantOrders(c.Request.Context(), &orderindexv1.ListMerchantOrdersRequest{
		MerchantId:    merchantID,
		Status:        q.status,
		PageSize:      q.pageSize,
		BeforeOrderId: q.before,
	})
	if err != nil {
		s.writeGRPCError(c, err)
		return
	}

	out := oas.MerchantOrderList{Orders: make([]oas.MerchantOrder, 0, len(resp.GetOrders()))}
	for _, o := range resp.GetOrders() {
		out.Orders = append(out.Orders, toMerchantOrder(o))
	}
	out.NextCursor = nextCursor(resp.GetNextBeforeOrderId())
	writeJSON(c, http.StatusOK, out)
}
