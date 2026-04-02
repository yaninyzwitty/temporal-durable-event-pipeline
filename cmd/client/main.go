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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
		getResp, err := userClient.GetUser(ctx, &userv1.GetUserRequest{
			Id: userCreateResp.User.Id,
		})
		if err != nil {
			log.Error("GetUser failed", "error", err)
		} else {
			log.Info("GetUser succeeded", "user", formatUser(getResp.User))
		}
	}

	log.Info("=== GetUserByEmail ===")
	emailResp, err := userClient.GetUserByEmail(ctx, &userv1.GetUserByEmailRequest{
		Email: "testuser@example.com",
	})
	if err != nil {
		log.Error("GetUserByEmail failed", "error", err)
	} else {
		log.Info("GetUserByEmail succeeded", "user", formatUser(emailResp.User))
	}

	log.Info("=== UpdateUser ===")
	if userCreateResp != nil && userCreateResp.User != nil {
		updateResp, err := userClient.UpdateUser(ctx, &userv1.UpdateUserRequest{
			Id:       userCreateResp.User.Id,
			Username: "updateduser",
			Email:    "updated@example.com",
		})
		if err != nil {
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
		getResp, err := productClient.GetProduct(ctx, &productv1.GetProductRequest{
			Id: productCreateResp.Product.Id,
		})
		if err != nil {
			log.Error("GetProduct failed", "error", err)
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

	payload, _ := structpb.NewStruct(map[string]interface{}{
		"action": "user_signup",
		"email":  "testuser@example.com",
	})

	log.Info("=== CreateEvent ===")
	eventCreateResp, err := eventClient.CreateEvent(ctx, &eventv1.CreateEventRequest{
		EventType: "user.signup",
		Payload:   payload,
	})
	if err != nil {
		log.Error("CreateEvent failed", "error", err)
	} else {
		log.Info("CreateEvent succeeded", "event", formatEvent(eventCreateResp.Event))
	}

	log.Info("=== GetEvent ===")
	if eventCreateResp != nil && eventCreateResp.Event != nil {
		getResp, err := eventClient.GetEvent(ctx, &eventv1.GetEventRequest{
			Id: eventCreateResp.Event.Id,
		})
		if err != nil {
			log.Error("GetEvent failed", "error", err)
		} else {
			log.Info("GetEvent succeeded", "event", formatEvent(getResp.Event))
		}
	}

	log.Info("=== ListEvents ===")
	listResp, err := eventClient.ListEvents(ctx, &eventv1.ListEventsRequest{
		Limit:  10,
		Offset: 0,
	})
	if err != nil {
		log.Error("ListEvents failed", "error", err)
	} else {
		log.Info("ListEvents succeeded", "count", len(listResp.Events))
		for _, e := range listResp.Events {
			log.Info("event", "data", formatEvent(e))
		}
	}

	log.Info("=== PollPendingEvents ===")
	pollResp, err := eventClient.PollPendingEvents(ctx, &eventv1.PollPendingEventsRequest{
		Limit: 10,
	})
	if err != nil {
		log.Error("PollPendingEvents failed", "error", err)
	} else {
		log.Info("PollPendingEvents succeeded", "count", len(pollResp.Events))
	}

	log.Info("=== UpdateEventStatus ===")
	if eventCreateResp != nil && eventCreateResp.Event != nil {
		updateResp, err := eventClient.UpdateEventStatus(ctx, &eventv1.UpdateEventStatusRequest{
			Id:     eventCreateResp.Event.Id,
			Status: eventv1.EventStatus_EVENT_STATUS_COMPLETED,
		})
		if err != nil {
			log.Error("UpdateEventStatus failed", "error", err)
		} else {
			log.Info("UpdateEventStatus succeeded", "event", formatEvent(updateResp.Event))
		}
	}
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
	return fmt.Sprintf("{id: %s, type: %s, status: %s}", e.Id, e.EventType, e.Status)
}

func getEnvOrDefault(envKey, defaultValue string) string {
	if value, exists := os.LookupEnv(envKey); exists {
		return value
	}
	return defaultValue
}
