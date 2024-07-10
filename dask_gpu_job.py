import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import torchvision
import torchvision.transforms as transforms
from nyctaxi_data_eng import *


# Select what hardware to use, default to CPU
device = torch.device("cpu")
# if torch.cuda.is_available():
#     device = torch.device("cuda")        # NVIDIA GPU
# elif torch.backends.mps.is_available():
#     device = torch.device("mps")         # Apple silicon GPU
# else:
#     device = torch.device("cpu")         # CPU
print(f"{device=}")


transform = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2470, 0.2435, 0.2616)),
    ]
)

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(3, 2_500, 5)
        self.pool = nn.MaxPool2d(2, 2)
        self.conv2 = nn.Conv2d(2_500, 16, 5)
        self.fc1 = nn.Linear(16 * 5 * 5, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 10)

    def forward(self, x):
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = x.view(-1, 16 * 5 * 5)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x

model = Net()
model = model.to(device)

# Use built-in PyTorch dataloaders for simplicity.
trainset = torchvision.datasets.CIFAR10(
    root="./data",
    train=True,
    download=True,
    transform=transform,
)
# reducing batch size helps with OOM errors, above 200 is dangerous
trainloader = torch.utils.data.DataLoader(
    trainset,
    batch_size=200,
    shuffle=True,
)
criterion = nn.CrossEntropyLoss()
optimizer = optim.SGD(model.parameters(), lr=0.001, momentum=0.9)

# Train model on 10 passes over the data
for epoch in range(10):
    print(f"Epoch {epoch}")
    for i, data in enumerate(trainloader, 0):
        inputs = data[0].to(device)
        labels = data[1].to(device)
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, labels)
        torch.cuda.empty_cache()
        loss.backward()
        optimizer.step()

# Save to file ind data_path
torch.save(model, data_path + "model.pt")

print(datetime.datetime.now() - today)
