package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	eventv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/event/v1"
	orderv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/order/v1"
	productv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/product/v1"
	userv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

func main() {
	addr := flag.String("addr", getEnvOrDefault("GRPC_ADDR", "localhost:50051"), "gRPC server address")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Error("failed to connect to gRPC server", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	userClient := userv1.NewUserServiceClient(conn)
	productClient := productv1.NewProductServiceClient(conn)
	orderClient := orderv1.NewOrderServiceClient(conn)
	eventClient := eventv1.NewEventServiceClient(conn)

	rpcCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 30*time.Second)
	}

	ctx, cancel := rpcCtx()
	defer cancel()

	// ---------- UserService: Create ----------

	log.Info("=== CreateUser ===")
	userCreateResp, err := userClient.CreateUser(ctx, &userv1.CreateUserRequest{
		Username: "testuser",
		Email:    "testuser@example.com",
		Password: "securepassword123",
	})
	if err != nil {
		log.Error("CreateUser failed", "error", err)
	} else {
		log.Info("CreateUser succeeded", "user", formatUser(userCreateResp.User))
	}

	log.Info("=== ListUsers ===")
	userListResp, err := userClient.ListUsers(ctx, &userv1.ListUsersRequest{})
	if err != nil {
		log.Error("ListUsers failed", "error", err)
	} else {
		log.Info("ListUsers succeeded", "count", len(userListResp.Users))
		for _, u := range userListResp.Users {
			log.Info("user", "data", formatUser(u))
		}
	}

	log.Info("=== GetUser ===")
	if userCreateResp != nil && userCreateResp.User != nil {
		getResp, errGet := userClient.GetUser(ctx, &userv1.GetUserRequest{
			Id: userCreateResp.User.Id,
		})
		if errGet != nil {
			log.Error("GetUser failed", "error", errGet)
		} else {
			log.Info("GetUser succeeded", "user", formatUser(getResp.User))
		}
	}

	log.Info("=== GetUserByEmail ===")
	emailResp, errEmail := userClient.GetUserByEmail(ctx, &userv1.GetUserByEmailRequest{
		Email: "testuser@example.com",
	})
	if errEmail != nil {
		log.Error("GetUserByEmail failed", "error", errEmail)
	} else {
		log.Info("GetUserByEmail succeeded", "user", formatUser(emailResp.User))
	}

	log.Info("=== UpdateUser ===")
	if userCreateResp != nil && userCreateResp.User != nil {
		updateResp, errUpdate := userClient.UpdateUser(ctx, &userv1.UpdateUserRequest{
			Id:       userCreateResp.User.Id,
			Username: "updateduser",
			Email:    "updated@example.com",
		})
		if errUpdate != nil {
			log.Error("UpdateUser failed", "error", err)
		} else {
			log.Info("UpdateUser succeeded", "user", formatUser(updateResp.User))
		}
	}

	// ---------- ProductService: Create ----------

	log.Info("=== CreateProduct ===")
	productCreateResp, err := productClient.CreateProduct(ctx, &productv1.CreateProductRequest{
		Name:          "Test Product",
		Description:   "A test product for gRPC testing",
		Price:         "29.99",
		StockQuantity: 100,
	})
	if err != nil {
		log.Error("CreateProduct failed", "error", err)
	} else {
		log.Info("CreateProduct succeeded", "product", formatProduct(productCreateResp.Product))
	}

	log.Info("=== ListProducts ===")
	productListResp, err := productClient.ListProducts(ctx, &productv1.ListProductsRequest{})
	if err != nil {
		log.Error("ListProducts failed", "error", err)
	} else {
		log.Info("ListProducts succeeded", "count", len(productListResp.Products))
		for _, p := range productListResp.Products {
			log.Info("product", "data", formatProduct(p))
		}
	}

	log.Info("=== GetProduct ===")
	if productCreateResp != nil && productCreateResp.Product != nil {
		getResp, errGet := productClient.GetProduct(ctx, &productv1.GetProductRequest{
			Id: productCreateResp.Product.Id,
		})
		if errGet != nil {
			log.Error("GetProduct failed", "error", errGet)
		} else {
			log.Info("GetProduct succeeded", "product", formatProduct(getResp.Product))
		}
	}

	log.Info("=== UpdateProduct ===")
	if productCreateResp != nil && productCreateResp.Product != nil {
		updateResp, err := productClient.UpdateProduct(ctx, &productv1.UpdateProductRequest{
			Id:            productCreateResp.Product.Id,
			Name:          "Updated Product",
			Description:   "An updated product description",
			Price:         "49.99",
			StockQuantity: 200,
		})
		if err != nil {
			log.Error("UpdateProduct failed", "error", err)
		} else {
			log.Info("UpdateProduct succeeded", "product", formatProduct(updateResp.Product))
		}
	}

	// ---------- OrderService ----------

	log.Info("=== CreateOrder ===")
	var orderID string
	if userCreateResp != nil && userCreateResp.User != nil {
		orderCreateResp, err := orderClient.CreateOrder(ctx, &orderv1.CreateOrderRequest{
			UserId:      userCreateResp.User.Id,
			TotalAmount: "59.98",
		})
		if err != nil {
			log.Error("CreateOrder failed", "error", err)
		} else {
			orderID = orderCreateResp.Order.Id
			log.Info("CreateOrder succeeded", "order", formatOrder(orderCreateResp.Order))
		}
	}

	log.Info("=== CreateOrderItem ===")
	if orderID != "" && productCreateResp != nil && productCreateResp.Product != nil {
		itemResp, err := orderClient.CreateOrderItem(ctx, &orderv1.CreateOrderItemRequest{
			OrderId:   orderID,
			ProductId: productCreateResp.Product.Id,
			Quantity:  2,
			UnitPrice: "29.99",
		})
		if err != nil {
			log.Error("CreateOrderItem failed", "error", err)
		} else {
			log.Info("CreateOrderItem succeeded", "item", formatOrderItem(itemResp.OrderItem))
		}
	}

	log.Info("=== GetOrder ===")
	if orderID != "" {
		getResp, err := orderClient.GetOrder(ctx, &orderv1.GetOrderRequest{
			Id: orderID,
		})
		if err != nil {
			log.Error("GetOrder failed", "error", err)
		} else {
			log.Info("GetOrder succeeded", "order", formatOrder(getResp.Order))
		}
	}

	log.Info("=== GetOrderItems ===")
	if orderID != "" {
		itemsResp, err := orderClient.GetOrderItems(ctx, &orderv1.GetOrderItemsRequest{
			OrderId: orderID,
		})
		if err != nil {
			log.Error("GetOrderItems failed", "error", err)
		} else {
			log.Info("GetOrderItems succeeded", "count", len(itemsResp.OrderItems))
			for _, item := range itemsResp.OrderItems {
				log.Info("order item", "data", formatOrderItem(item))
			}
		}
	}

	log.Info("=== ListOrdersByUser ===")
	if userCreateResp != nil && userCreateResp.User != nil {
		listResp, err := orderClient.ListOrdersByUser(ctx, &orderv1.ListOrdersByUserRequest{
			UserId: userCreateResp.User.Id,
		})
		if err != nil {
			log.Error("ListOrdersByUser failed", "error", err)
		} else {
			log.Info("ListOrdersByUser succeeded", "count", len(listResp.Orders))
			for _, o := range listResp.Orders {
				log.Info("order", "data", formatOrder(o))
			}
		}
	}

	log.Info("=== UpdateOrderStatus ===")
	if orderID != "" {
		updateResp, err := orderClient.UpdateOrderStatus(ctx, &orderv1.UpdateOrderStatusRequest{
			Id:     orderID,
			Status: "completed",
		})
		if err != nil {
			log.Error("UpdateOrderStatus failed", "error", err)
		} else {
			log.Info("UpdateOrderStatus succeeded", "order", formatOrder(updateResp.Order))
		}
	}

	// ---------- Cleanup: delete in dependency order ----------

	log.Info("=== DeleteOrderItem ===")
	if orderID != "" {
		itemsResp, err := orderClient.GetOrderItems(ctx, &orderv1.GetOrderItemsRequest{
			OrderId: orderID,
		})
		if err != nil {
			log.Error("GetOrderItems for delete failed", "error", err)
		} else if len(itemsResp.OrderItems) > 0 {
			deleteItemResp, err := orderClient.DeleteOrderItem(ctx, &orderv1.DeleteOrderItemRequest{
				Id: itemsResp.OrderItems[0].Id,
			})
			if err != nil {
				log.Error("DeleteOrderItem failed", "error", err)
			} else {
				log.Info("DeleteOrderItem succeeded", "response", deleteItemResp)
			}
		}
	}

	log.Info("=== DeleteOrder ===")
	if orderID != "" {
		deleteResp, err := orderClient.DeleteOrder(ctx, &orderv1.DeleteOrderRequest{
			Id: orderID,
		})
		if err != nil {
			log.Error("DeleteOrder failed", "error", err)
		} else {
			log.Info("DeleteOrder succeeded", "response", deleteResp)
		}
	}

	log.Info("=== DeleteProduct ===")
	if productCreateResp != nil && productCreateResp.Product != nil {
		deleteResp, err := productClient.DeleteProduct(ctx, &productv1.DeleteProductRequest{
			Id: productCreateResp.Product.Id,
		})
		if err != nil {
			log.Error("DeleteProduct failed", "error", err)
		} else {
			log.Info("DeleteProduct succeeded", "response", deleteResp)
		}
	}

	log.Info("=== DeleteUser ===")
	if userCreateResp != nil && userCreateResp.User != nil {
		deleteResp, err := userClient.DeleteUser(ctx, &userv1.DeleteUserRequest{
			Id: userCreateResp.User.Id,
		})
		if err != nil {
			log.Error("DeleteUser failed", "error", err)
		} else {
			log.Info("DeleteUser succeeded", "response", deleteResp)
		}
	}

	// ---------- EventService ----------

	payload, err := structpb.NewStruct(map[string]any{
		"action": "user_signup",
		"email":  "testuser@example.com",
	})
	if err != nil {
		log.Error("Failed to create struct payload", "error", err)
		return
	}

	log.Info("=== CreateEvent (triggers outbox pattern) ===")
	eventCreateResp, err := eventClient.CreateEvent(ctx, &eventv1.CreateEventRequest{
		EventType: "user.signup",
		Payload:   payload,
	})
	if err != nil {
		log.Error("CreateEvent failed", "error", err)
		os.Exit(1)
	}
	log.Info("CreateEvent succeeded", "event", formatEvent(eventCreateResp.Event))

	log.Info("Waiting for outbox to process event (check Redpanda console at http://localhost:8080)...")

	pollCtx, pollCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer pollCancel()

	var finalEvent *eventv1.Event
	for pollCtx.Err() == nil {
		getResp, err := eventClient.GetEvent(pollCtx, &eventv1.GetEventRequest{
			Id: eventCreateResp.Event.Id,
		})
		if err != nil {
			log.Error("GetEvent failed", "error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		finalEvent = getResp.Event
		log.Debug("Poll check", "status", finalEvent.StatusV2)

		if finalEvent.StatusV2 == eventv1.EventStatus_EVENT_STATUS_COMPLETED {
			log.Info("SUCCESS: Event published to Redpanda and status updated to COMPLETED",
				"event", formatEvent(finalEvent))
			break
		}

		if finalEvent.StatusV2 == eventv1.EventStatus_EVENT_STATUS_FAILED {
			log.Error("FAILED: Event marked as failed", "event", formatEvent(finalEvent))
			os.Exit(1)
		}

		time.Sleep(500 * time.Millisecond)
	}

	if pollCtx.Err() != nil {
		log.Error("Timeout waiting for event processing", "lastEvent", formatEvent(finalEvent))
		os.Exit(1)
	}

	log.Info("=== Verify: Check topic in Redpanda Console ===")
	log.Info("Open http://localhost:8080 to verify topic 'temporal-pipeline.user.signup' has messages")
}

func formatUser(u *userv1.User) string {
	if u == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{id: %s, username: %s, email: %s}", u.Id, u.Username, u.Email)
}

func formatProduct(p *productv1.Product) string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{id: %s, name: %s, price: %s, stock: %d}", p.Id, p.Name, p.Price, p.StockQuantity)
}

func formatOrder(o *orderv1.Order) string {
	if o == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{id: %s, user_id: %s, status: %s, total: %s}", o.Id, o.UserId, o.Status, o.TotalAmount)
}

func formatOrderItem(oi *orderv1.OrderItem) string {
	if oi == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{id: %s, order_id: %s, product_id: %s, qty: %d, price: %s}", oi.Id, oi.OrderId, oi.ProductId, oi.Quantity, oi.UnitPrice)
}

func formatEvent(e *eventv1.Event) string {
	if e == nil {
		return "<nil>"
	}
	//nolint:staticcheck // SA1019: e.Status is deprecated
	return fmt.Sprintf("{id: %s, type: %s, status: %s}", e.Id, e.EventType, e.Status)
}

func getEnvOrDefault(envKey, defaultValue string) string {
	if value, exists := os.LookupEnv(envKey); exists {
		return value
	}
	return defaultValue
}
