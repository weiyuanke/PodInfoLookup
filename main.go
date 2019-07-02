package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	v1 "k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	"time"
)

var (
	//集群配置文件路径
	kubeconfigStr = flag.String("kubeconfig", "default value", "kubernetes config file")
	//存储Pod的信息，实现了Storage接口
	podIndexer cache.Indexer
	//控制器，监听API server，Pod资源有变化时，会调用相关的回调函数（Add、Update、Delete）
	podController cache.Controller
	//回调函数将接收到的事件先放入workQueue，单独一个gorouting再消费workQueue
	workQueue workqueue.RateLimitingInterface
	//kubernetes clientset
	clientSet *kubernetes.Clientset
	//dynamic client
	dynamicClient dynamic.Interface
	//testrc group、version、Resource
	testrcGVR = schema.GroupVersionResource{
		Group:    "stable.example.com",
		Version:  "v1",
		Resource: "testcrs",
	}
)

/*
 get clientset to access kubernetes api server
 */
func getClientSet(kubeconfigStr string) *kubernetes.Clientset {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigStr)
	if err != nil {
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	return clientset
}

/**
 动态client，用以操作custom resource
 */
func getDynamicClient(kubeconfigStr string) dynamic.Interface {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigStr)
	if err != nil {
		panic(err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	return dynamicClient
}

func main()  {
	//解析参数
	flag.Parse()

	//构造clientset
	clientSet = getClientSet(*kubeconfigStr)

	//
	dynamicClient = getDynamicClient(*kubeconfigStr)

	//work queue
	workQueue = workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "podsqueue")

	//构造一个podController, 该Controller监听系统中所有的pod变化
	podIndexer, podController = cache.NewIndexerInformer(
		&cache.ListWatch{
			//初次的List
			ListFunc: func(options v12.ListOptions) (object runtime.Object, e error) {
				return clientSet.CoreV1().Pods("default").List(options)
			},
			//后续变化的监听
			WatchFunc: func(options v12.ListOptions) (i watch.Interface, e error) {
				return clientSet.CoreV1().Pods("default").Watch(options)
			},
		},
		&v1.Pod{},
		//resync is not needed
		0,
		//只处理创建和删除事件
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
				if err != nil {
					fmt.Printf("Couldn't get key for object %+v: %v", obj, err)
					return
				}

				workQueue.Add(key)
			},

			DeleteFunc: func(obj interface{}) {
				key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
				if err != nil {
					fmt.Printf("Couldn't get key for object %+v: %v", obj, err)
					return
				}
				workQueue.Add(key)
			},
		},
		cache.Indexers{},
	)

	var stopCh <-chan struct{}
	//非阻塞
	go podController.Run(stopCh)

	// wait for the controller to List finish, this will block until list finish
	cache.WaitForCacheSync(stopCh, podController.HasSynced)

	//启动消费线程
	wait.Until(worker, time.Second, stopCh)
}

/**
 消费线程，处理系统中相关的Pod
 */
func worker() {
	for {
		//采用匿名函数主要是为了用defer
		func () {
			key, quit := workQueue.Get()
			if quit {
				fmt.Println("queue shutdown")
				return
			}

			defer workQueue.Done(key)

			obj, _, err := podIndexer.GetByKey(key.(string))
			if err != nil {
				fmt.Println("errror cccc")
				return
			}

			//动态client，可以用来操作自定义资源
			res := dynamicClient.Resource(testrcGVR)

			if obj != nil {
				//add
				fmt.Println("add: ", key)
				currentPod := obj.(*v1.Pod)
				tct := NewTestcr(md5V(key.(string)), currentPod.Name, currentPod.Status.PodIP, currentPod.UID)
				res.Create(tct, v12.CreateOptions{})
			} else {
				//delete
				fmt.Println("delete: ", key)
				res.Delete(md5V(key.(string)), &v12.DeleteOptions{})
			}

			//当前的自定义资源(Testcr)列表
			unlist, err := res.List(v12.ListOptions{})
			if err != nil {
				fmt.Println(err)
				return
			}

			fmt.Println("\ncurrent list:")
			unlist.EachListItem(func(object runtime.Object) error {
				fmt.Println(object)
				return nil
			})
		}()
	}
}

func NewTestcr(name, ip, uid, key interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "Testcr",
			"apiVersion": "stable.example.com/v1",
			"metadata": map[string]interface{}{
				"name": name,
				"namespace": "default",
			},
			"spec": map[string]interface{}{
				"podip": ip,
				"poduid": uid,
				"podkey": key,
				"podname": name,
			},
		},
	}
}

func md5V(str string) string  {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}



