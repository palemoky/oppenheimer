package httpapi

import (
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
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

// pathID 解析路径参数，失败时已写出 400。
func pathID(c *gin.Context, name string) (int64, bool) {
	n, ok := parseID(c.Param(name))
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

func (s *server) createOrder(c *gin.Context) {
	key := c.GetHeader(idempotencyKeyHeader)
	if key == "" || len(key) > maxIdempotencyKeyLen {
		badRequest(c, idempotencyKeyHeader+" header is required and must be 1 to "+strconv.Itoa(maxIdempotencyKeyLen)+" characters")
		return
	}
	var req createOrderRequest
	if !decodeBody(c, &req, false) {
		return
	}
	if len(req.Items) == 0 {
		badRequest(c, "items must not be empty")
		return
	}
	items := make([]*orderv1.CreateOrderItem, 0, len(req.Items))
	for _, it := range req.Items {
		if it.SKUID <= 0 || it.Quantity <= 0 {
			badRequest(c, "each item needs a positive sku_id and quantity")
			return
		}
		items = append(items, &orderv1.CreateOrderItem{SkuId: int64(it.SKUID), Quantity: it.Quantity})
	}

	resp, err := s.deps.Order.CreateOrder(c.Request.Context(), &orderv1.CreateOrderRequest{
		UserId:         userID(c),
		IdempotencyKey: key,
		Items:          items,
	})
	if err != nil {
		s.writeGRPCError(c, err)
		return
	}
	if resp.GetOrder() == nil {
		s.writeEmptyResponse(c, "CreateOrder")
		return
	}
	writeJSON(c, http.StatusCreated, toOrder(resp.GetOrder()))
}

func (s *server) getOrder(c *gin.Context) {
	orderID, ok := pathID(c, "order_id")
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

func (s *server) cancelOrder(c *gin.Context) {
	orderID, ok := pathID(c, "order_id")
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

func (s *server) payOrder(c *gin.Context) {
	orderID, ok := pathID(c, "order_id")
	if !ok {
		return
	}
	var req payOrderRequest
	if !decodeBody(c, &req, true) {
		return
	}
	var outcome paymentv1.SimulatedOutcome
	switch req.Outcome {
	case "":
		outcome = paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_UNSPECIFIED
	case "success":
		outcome = paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS
	case "failure":
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

func (s *server) getPayment(c *gin.Context) {
	orderID, ok := pathID(c, "order_id")
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

func (s *server) getStock(c *gin.Context) {
	skuID, ok := pathID(c, "sku_id")
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
